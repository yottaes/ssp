use std::fmt;

use bytemuck::{Pod, Zeroable};

#[derive(Pod, Zeroable, Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(C)]
pub struct Pubkey([u8; 32]);

impl Pubkey {
    pub const TOKEN_PROGRAM: Self = Self([
        6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133,
        237, 95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
    ]);

    /// Decode a base58 string into a Pubkey.
    pub fn from_b58(s: &str) -> Result<Self, anyhow::Error> {
        let mut buf = [0u8; 32];
        bs58::decode(s).onto(&mut buf)?;
        Ok(Self(buf))
    }

    /// Decode an optional base58 string. Returns Ok(None) if input is None.
    pub fn try_from_b58(s: Option<&str>) -> Result<Option<Self>, anyhow::Error> {
        s.map(Self::from_b58).transpose()
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 32]
    }
}

impl fmt::Display for Pubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl From<[u8; 32]> for Pubkey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Lets Pubkey be used directly where &[u8] is expected (e.g. BinaryArray).
impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
