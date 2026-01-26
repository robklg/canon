use std::io::Write;

/// Progress indicator for long-running operations
pub struct Progress {
    total: usize,
    interval: usize,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        Self {
            total,
            interval: std::cmp::max(total / 20, 1),
        }
    }

    pub fn update(&self, i: usize) {
        if i > 0 && i % self.interval == 0 {
            let pct = (i * 100) / self.total;
            eprint!("\r  {}% ({}/{})", pct, i, self.total);
            let _ = std::io::stderr().flush();
        }
    }

    pub fn finish(&self) {
        if self.total > self.interval {
            eprint!("\r  100% ({}/{})\n", self.total, self.total);
        }
    }
}
