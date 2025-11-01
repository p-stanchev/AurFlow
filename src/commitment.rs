use std::fmt;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

impl Commitment {
    pub const ALL: [Commitment; 3] = [
        Commitment::Processed,
        Commitment::Confirmed,
        Commitment::Finalized,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Commitment::Processed => "processed",
            Commitment::Confirmed => "confirmed",
            Commitment::Finalized => "finalized",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "processed" => Some(Commitment::Processed),
            "confirmed" => Some(Commitment::Confirmed),
            "finalized" => Some(Commitment::Finalized),
            _ => None,
        }
    }

    pub fn merge_requirement(self, other: Option<Self>) -> Self {
        match other {
            Some(other) if other > self => other,
            _ => self,
        }
    }
}

impl fmt::Display for Commitment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
