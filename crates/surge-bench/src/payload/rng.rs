pub(super) struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    pub(super) fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub(super) fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i < buf.len() {
            let val = self.next_u64().to_le_bytes();
            let remaining = buf.len() - i;
            let to_copy = remaining.min(8);
            buf[i..i + to_copy].copy_from_slice(&val[..to_copy]);
            i += to_copy;
        }
    }
}

pub(super) fn mix_seed(seed: u64, name: &str) -> u64 {
    let mut mixed = seed ^ 0x9e37_79b9_7f4a_7c15;
    for byte in name.bytes() {
        mixed = mixed.rotate_left(9) ^ u64::from(byte);
        mixed = mixed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    }
    mixed
}
