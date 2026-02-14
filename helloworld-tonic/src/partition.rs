#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefixRange {
    pub start: char,
    pub end: char,
}

impl PrefixRange {
    pub fn new(start: char, end: char) -> Result<Self, String> {
        let start = start.to_ascii_lowercase();
        let end = end.to_ascii_lowercase();

        if !(('a'..='z').contains(&start) && ('a'..='z').contains(&end)) {
            return Err(format!("range must be within a-z, got {}-{}", start, end));
        }
        if start > end {
            return Err(format!("range start must be <= end, got {}-{}", start, end));
        }
        Ok(Self { start, end })
    }

    // Accepts "a-z" / "a-a" (single char ranges must still be "a-a").
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (start, end) = s
            .split_once('-')
            .ok_or_else(|| format!("invalid range {}, expected a-z", s))?;

        let start = start
            .trim()
            .chars()
            .next()
            .ok_or_else(|| format!("invalid range {}, empty start", s))?;
        let end = end
            .trim()
            .chars()
            .next()
            .ok_or_else(|| format!("invalid range {}, empty end", s))?;

        Self::new(start, end)
    }

    pub fn contains_first_char_of(&self, s: &str) -> bool {
        let c = match s.chars().next() {
            Some(c) => c.to_ascii_lowercase(),
            None => return false,
        };
        self.contains_char(c)
    }

    pub fn contains_char(&self, c: char) -> bool {
        let c = c.to_ascii_lowercase();
        ('a'..='z').contains(&c) && c >= self.start && c <= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub range: PrefixRange,
    pub backends: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMap {
    partitions: Vec<Partition>,
}

impl PartitionMap {
    pub fn new(partitions: Vec<Partition>) -> Result<Self, String> {
        for p in &partitions {
            if p.backends.is_empty() {
                return Err(format!("partition {:?} has no backends", p.range));
            }
        }
        Ok(Self { partitions })
    }

    pub fn route(&self, prefix: &str) -> Option<&Partition> {
        self.partitions
            .iter()
            .find(|p| p.range.contains_first_char_of(prefix))
    }

    // Format: "a-i=http://[::1]:50051,j-r=http://[::1]:50053,s-z=http://[::1]:50054"
    // Backends can be a '|' separated list (Phase 2+), e.g. "a-i=addr1|addr2|addr3".
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("partition map is empty".to_string());
        }

        let mut partitions = Vec::new();
        for entry in s.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            let (range_str, backends_str) = entry
                .split_once('=')
                .ok_or_else(|| format!("invalid partition entry {}, expected range=backend", entry))?;

            let range = PrefixRange::parse(range_str)?;
            let backends: Vec<String> = backends_str
                .split('|')
                .map(|b| b.trim())
                .filter(|b| !b.is_empty())
                .map(|b| b.to_string())
                .collect();

            partitions.push(Partition { range, backends });
        }

        Self::new(partitions)
    }
}

pub fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_range_parse_and_contains() {
        let r = PrefixRange::parse("a-i").unwrap();
        assert!(r.contains_char('a'));
        assert!(r.contains_char('i'));
        assert!(r.contains_char('B'));
        assert!(!r.contains_char('j'));
        assert!(!r.contains_first_char_of(""));
    }

    #[test]
    fn partition_map_routes_by_first_char() {
        let pm = PartitionMap::parse("a-i=http://n1,j-r=http://n2,s-z=http://n3").unwrap();
        assert_eq!(pm.route("apple").unwrap().backends[0], "http://n1");
        assert_eq!(pm.route("Mango").unwrap().backends[0], "http://n2");
        assert_eq!(pm.route("zoo").unwrap().backends[0], "http://n3");
        assert!(pm.route("").is_none());
        assert!(pm.route("1abc").is_none());
    }
}

