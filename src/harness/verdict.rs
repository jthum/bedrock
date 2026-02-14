use serde::{Deserialize, Serialize};
use std::fmt;

/// The result of a harness hook evaluation.
///
/// Every harness hook returns one of three verdicts:
/// - `Allow` — Proceed normally
/// - `Reject(reason)` — Block the action, feed reason to LLM
/// - `Escalate(reason)` — Pause and ask a human
#[must_use]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "reason", rename_all = "snake_case")]
pub enum Verdict {
    /// Proceed with the action.
    Allow,
    /// Block the action with a reason string.
    /// The reason is injected into the LLM context as structured feedback.
    Reject(String),
    /// Pause execution and escalate to a human for approval.
    /// Pause execution and escalate to a human for approval.
    Escalate(String),
    /// Modify the parameters or content of the action.
    Modify(serde_json::Value),
}

impl Verdict {
    /// Returns `true` if the verdict allows the action to proceed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Verdict::Allow)
    }

    /// Returns `true` if the verdict blocks the action.
    pub fn is_rejected(&self) -> bool {
        matches!(self, Verdict::Reject(_))
    }

    /// Returns `true` if the verdict requires human escalation.
    pub fn is_escalated(&self) -> bool {
        matches!(self, Verdict::Escalate(_))
    }

    /// Returns `true` if the verdict modifies the action.
    pub fn is_modified(&self) -> bool {
        matches!(self, Verdict::Modify(_))
    }

    /// Get the reason string, if any.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Verdict::Allow => None,
            Verdict::Reject(reason) | Verdict::Escalate(reason) => Some(reason),
            Verdict::Modify(_) => None,
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Allow => write!(f, "ALLOW"),
            Verdict::Reject(reason) => write!(f, "REJECT: {}", reason),
            Verdict::Escalate(reason) => write!(f, "ESCALATE: {}", reason),
            Verdict::Modify(val) => write!(f, "MODIFY: {}", val),
        }
    }
}

/// Compose multiple verdicts using first-reject-wins semantics.
///
/// - If any verdict is `Reject`, return that `Reject`.
/// - If any verdict is `Escalate` (and none rejected), return that `Escalate`.
/// - If all verdicts are `Allow`, return `Allow`.
pub fn compose_verdicts(verdicts: &[Verdict]) -> Verdict {
    // First pass: check for rejections
    for v in verdicts {
        if let Verdict::Reject(reason) = v {
            return Verdict::Reject(reason.clone());
        }
    }
    // Second pass: check for escalations
    for v in verdicts {
        if let Verdict::Escalate(reason) = v {
            return Verdict::Escalate(reason.clone());
        }
    }
    // Third pass: check for modifications
    for v in verdicts {
        if let Verdict::Modify(val) = v {
            return Verdict::Modify(val.clone());
        }
    }
    Verdict::Allow
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verdict_allow() {
        let v = Verdict::Allow;
        assert!(v.is_allowed());
        assert!(!v.is_rejected());
        assert!(!v.is_escalated());
        assert_eq!(v.reason(), None);
        assert_eq!(format!("{}", v), "ALLOW");
    }

    #[test]
    fn test_verdict_reject() {
        let v = Verdict::Reject("too dangerous".to_string());
        assert!(!v.is_allowed());
        assert!(v.is_rejected());
        assert_eq!(v.reason(), Some("too dangerous"));
        assert_eq!(format!("{}", v), "REJECT: too dangerous");
    }

    #[test]
    fn test_verdict_escalate() {
        let v = Verdict::Escalate("needs approval".to_string());
        assert!(!v.is_allowed());
        assert!(v.is_escalated());
        assert_eq!(v.reason(), Some("needs approval"));
    }

    #[test]
    fn test_compose_all_allow() {
        let verdicts = vec![Verdict::Allow, Verdict::Allow, Verdict::Allow];
        assert_eq!(compose_verdicts(&verdicts), Verdict::Allow);
    }

    #[test]
    fn test_compose_reject_wins() {
        let verdicts = vec![
            Verdict::Allow,
            Verdict::Reject("blocked".to_string()),
            Verdict::Allow,
        ];
        assert_eq!(
            compose_verdicts(&verdicts),
            Verdict::Reject("blocked".to_string())
        );
    }

    #[test]
    fn test_compose_reject_over_escalate() {
        let verdicts = vec![
            Verdict::Escalate("ask human".to_string()),
            Verdict::Reject("blocked".to_string()),
        ];
        assert_eq!(
            compose_verdicts(&verdicts),
            Verdict::Reject("blocked".to_string())
        );
    }

    #[test]
    fn test_compose_escalate_when_no_reject() {
        let verdicts = vec![
            Verdict::Allow,
            Verdict::Escalate("ask human".to_string()),
            Verdict::Allow,
        ];
        assert_eq!(
            compose_verdicts(&verdicts),
            Verdict::Escalate("ask human".to_string())
        );
    }

    #[test]
    fn test_compose_empty() {
        let verdicts: Vec<Verdict> = vec![];
        assert_eq!(compose_verdicts(&verdicts), Verdict::Allow);
    }
}
