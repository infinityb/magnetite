pub struct PieceLength {
    exponent: u8,
}

impl PieceLength {
    pub fn new(exponent: u8) -> Self {
        PieceLength { exponent: exponent }
    }

    pub fn from_val(mut value: u64) -> Option<Self> {
        if value == 0 {
            return None;
        }
        let mut exponent = 0;
        while value != 1 {
            value = value >> 1;
            exponent += 1;
        }
        Some(PieceLength { exponent: exponent })
    }

    pub fn offset(&self, piece: u32) -> u64 {
        (piece as u64) << self.exponent
    }

    pub fn length(&self) -> u32 {
        1 << self.exponent
    }

    pub fn piece_num(&self, offset: u64) -> u32 {
        (offset >> self.exponent) as u32
    }

    pub fn align(&self, offset: u64) -> u64 {
        let piece_len = 1 << self.exponent;
        (::std::u64::MAX ^ (piece_len - 1)) & offset
    }
}