//! A thin, best-effort parse of a JSON-RPC message: just enough to classify
//! it (has-method / has-id / has-result). Decoding failure is never fatal —
//! [`Envelope::parse`] returns `None` and the caller forwards verbatim.

/// A JSON-RPC request/response id. Lean and Helix use numeric ids; the proxy
/// injects string ids (a fixed prefix + counter), so both shapes matter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Id {
    Num(i64),
    Str(String),
}

/// The classifying fields of a JSON-RPC message. Other fields are ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Envelope {
    pub id: Option<Id>,
    pub method: Option<String>,
    pub has_result: bool,
    pub has_error: bool,
}

impl Envelope {
    /// Best-effort decode of a message body. `None` if the bytes are not a
    /// JSON object — caller forwards verbatim rather than treating it as fatal.
    pub fn parse(body: &[u8]) -> Option<Envelope> {
        let value: serde_json::Value = serde_json::from_slice(body).ok()?;
        let obj = value.as_object()?;

        let id = obj.get("id").and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_i64().map(Id::Num),
            serde_json::Value::String(s) => Some(Id::Str(s.clone())),
            _ => None, // null / other: treated as "no id"
        });
        let method = obj
            .get("method")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        Some(Envelope {
            id,
            method,
            has_result: obj.contains_key("result"),
            has_error: obj.contains_key("error"),
        })
    }

    /// method, no id.
    pub fn is_notification(&self) -> bool {
        self.method.is_some() && self.id.is_none()
    }

    /// method and id (a request, in either direction).
    pub fn is_request(&self) -> bool {
        self.method.is_some() && self.id.is_some()
    }

    /// id, no method (a response to some earlier request).
    pub fn is_response(&self) -> bool {
        self.method.is_none() && self.id.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_the_three_shapes() {
        let notif =
            Envelope::parse(br#"{"jsonrpc":"2.0","method":"textDocument/didChange"}"#).unwrap();
        assert!(notif.is_notification());
        assert!(!notif.is_request() && !notif.is_response());

        let req =
            Envelope::parse(br#"{"jsonrpc":"2.0","id":7,"method":"window/showMessage"}"#).unwrap();
        assert!(req.is_request());
        assert_eq!(req.id, Some(Id::Num(7)));

        let resp = Envelope::parse(br#"{"jsonrpc":"2.0","id":"lhv-3","result":{}}"#).unwrap();
        assert!(resp.is_response());
        assert!(resp.has_result);
        assert_eq!(resp.id, Some(Id::Str("lhv-3".into())));
    }

    #[test]
    fn non_json_is_none() {
        assert!(Envelope::parse(b"not json").is_none());
        assert!(Envelope::parse(b"[1,2,3]").is_none()); // array, not an object
    }

    #[test]
    fn null_id_is_no_id() {
        let e = Envelope::parse(br#"{"jsonrpc":"2.0","id":null,"method":"x"}"#).unwrap();
        assert!(e.is_notification());
    }
}
