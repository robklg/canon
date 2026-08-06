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
        if i > 0 && i.is_multiple_of(self.interval) {
            let pct = (i * 100) / self.total;
            eprint!("\r  {}% ({}/{})\x1b[K", pct, i, self.total);
            let _ = std::io::stderr().flush();
        }
    }

    pub fn update_with_name(&self, i: usize, name: &str) {
        if i > 0 && i.is_multiple_of(self.interval) {
            let pct = (i * 100) / self.total;
            // Truncate long filenames to keep the line readable
            let display_name = if name.len() > 40 {
                &name[name.len() - 40..]
            } else {
                name
            };
            eprint!("\r  {}% ({}/{}) {}\x1b[K", pct, i, self.total, display_name);
            let _ = std::io::stderr().flush();
        }
    }

    pub fn finish(&self) {
        if self.total > self.interval {
            eprint!("\r  100% ({}/{})\x1b[K\n", self.total, self.total);
        }
    }
}
