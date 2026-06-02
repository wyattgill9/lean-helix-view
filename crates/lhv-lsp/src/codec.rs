//! `Content-Length: <n>\r\n\r\n<body>` framing.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// One LSP frame, holding its complete on-wire representation.
///
/// `raw` is exactly the bytes seen on the wire: the header block, the blank
/// `\r\n` separator, and the body. `body_offset` marks where the body begins.
/// Forwarding writes [`Frame::as_bytes`] (verbatim); snooping reads
/// [`Frame::body`] (a borrow, decoded off the hot path).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    raw: Vec<u8>,
    body_offset: usize,
}

impl Frame {
    /// Build a frame from a JSON body, framing it with a canonical
    /// `Content-Length` header. Used for messages *we* originate (injected
    /// goal queries, viewer-socket payloads) — never for forwarded traffic,
    /// which is reconstructed verbatim by [`read_frame`].
    pub fn from_body(body: &[u8]) -> Frame {
        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        let body_offset = header.len();
        let mut raw = Vec::with_capacity(body_offset + body.len());
        raw.extend_from_slice(header.as_bytes());
        raw.extend_from_slice(body);
        Frame { raw, body_offset }
    }

    /// The complete on-wire bytes (headers + body). What forwarding writes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// The body bytes only. What snooping decodes.
    pub fn body(&self) -> &[u8] {
        &self.raw[self.body_offset..]
    }

    /// Consume the frame, yielding its on-wire bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.raw
    }
}

/// Read one frame.
///
/// Returns `Ok(None)` on a *clean* EOF at a frame boundary (no bytes pending) —
/// the graceful "stream closed" signal. EOF or malformed data mid-frame is an
/// `Err`, which callers treat as an abnormal close.
///
/// Unknown headers (e.g. `Content-Type`) are tolerated and preserved in the
/// returned frame's bytes; only `Content-Length` is interpreted.
pub async fn read_frame<R>(reader: &mut R) -> io::Result<Option<Frame>>
where
    R: AsyncBufRead + Unpin,
{
    let mut raw: Vec<u8> = Vec::new();
    let mut content_length: Option<usize> = None;
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            // EOF. Clean only if it lands exactly at a frame boundary.
            if raw.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF in the middle of a frame header",
            ));
        }
        raw.extend_from_slice(line.as_bytes());

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break; // blank line ends the header block
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                let len = value.trim().parse::<usize>().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("bad Content-Length: {e}"))
                })?;
                content_length = Some(len);
            }
            // other headers: tolerated, already preserved in `raw`
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("malformed header line: {trimmed:?}"),
            ));
        }
    }

    let len = content_length
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length"))?;
    let body_offset = raw.len();
    raw.resize(body_offset + len, 0);
    reader.read_exact(&mut raw[body_offset..]).await?;

    Ok(Some(Frame { raw, body_offset }))
}

/// Write a frame's on-wire bytes and flush. For forwarded frames this is a
/// verbatim copy; for [`Frame::from_body`] frames it emits canonical framing.
pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(frame.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    /// The transparency invariant at the codec level: bytes read, then
    /// written back, equal the original stream — including a frame carrying an
    /// extra `Content-Type` header and a frame whose body is not JSON.
    #[tokio::test]
    async fn roundtrip_is_byte_exact() {
        let mut input = Vec::new();
        input.extend_from_slice(
            Frame::from_body(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
                .as_bytes(),
        );
        input.extend_from_slice(Frame::from_body(b"this body is not json").as_bytes());
        // Hand-crafted frame with a second header, in non-canonical order.
        input.extend_from_slice(
            b"Content-Type: application/vscode-jsonrpc; charset=utf-8\r\nContent-Length: 2\r\n\r\n{}",
        );

        let mut reader = BufReader::new(&input[..]);
        let mut out = Vec::new();
        let mut count = 0;
        while let Some(frame) = read_frame(&mut reader).await.unwrap() {
            out.extend_from_slice(frame.as_bytes()); // forward verbatim
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(out, input, "forwarded bytes must equal received bytes");
    }

    #[tokio::test]
    async fn body_accessor_excludes_headers() {
        let frame = Frame::from_body(b"{}");
        assert_eq!(frame.body(), b"{}");
        let custom = b"X-Foo: bar\r\nContent-Length: 2\r\n\r\nhi";
        let mut reader = BufReader::new(&custom[..]);
        let frame = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(frame.body(), b"hi");
        assert_eq!(frame.as_bytes(), custom);
    }

    #[tokio::test]
    async fn clean_eof_at_boundary_is_none() {
        let empty: &[u8] = b"";
        let mut reader = BufReader::new(empty);
        assert!(read_frame(&mut reader).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncated_frame_is_error() {
        let partial = b"Content-Length: 50\r\n\r\nonly a few bytes";
        let mut reader = BufReader::new(&partial[..]);
        assert!(read_frame(&mut reader).await.is_err());
    }
}
