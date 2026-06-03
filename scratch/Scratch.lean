-- Park the cursor inside any `by` block to see goals in the infoview.

theorem add_zero_left (n : Nat) : 0 + n = n := by
  induction n with
  | zero => rfl 
  | succ k ih =>
    rw [Nat.add_succ, ih]   

theorem and_comm' (p q : Prop) : p ∧ q → q ∧ p := by
  intro h
  cases h with
  | into hp, hq
    exact ⟨hq, hp⟩

example (a b : Nat) : a + b = b + a := by
  -- cursor here: one goal, `a + b = b + a`
  rw [Nat.add_comm]

-- A deliberately-unfinished proof so you can see a live `sorry` goal.
example (n : Nat) : n + n = 2 * n := by
  sorry
