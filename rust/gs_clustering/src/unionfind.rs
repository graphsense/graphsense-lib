use std::sync::atomic::{AtomicU32, Ordering};

/// Lock-free union-find over `u32` ids that unions **by minimum**: linking
/// always attaches the larger root under the smaller, so a component's root
/// IS its minimum member — the canonical cluster label the whole system uses
/// (`cluster_id == min(address_id)`, the incremental path's smaller-root-
/// survives merge rule). No separate relabel pass is ever needed.
///
/// 4 bytes per id (one `AtomicU32` parent). Parent pointers only ever
/// decrease (initialised to self; links go larger→smaller; path halving
/// short-cuts to a grandparent, which is `<=` the parent), so chains strictly
/// descend, cycles are impossible and `find` terminates. Without union by
/// rank the amortised bound is O(log n) per operation — irrelevant here: the
/// bootstrap's ingest is ~0.3% of its wall-clock.
///
/// Concurrency follows the standard index-ordered lock-free design: a root is
/// linked with a CAS that only succeeds while it is still a root; a failed
/// CAS re-finds and retries, and every successful link reduces the root count
/// by exactly one, so the system makes progress. Path halving races are
/// benign (a lost CAS just means less compression).
pub struct MinUnionFind {
    parent: Vec<AtomicU32>,
}

impl MinUnionFind {
    pub fn new(n: usize) -> Self {
        assert!(n <= u32::MAX as usize + 1, "id space exceeds u32");
        let mut parent = Vec::with_capacity(n);
        for i in 0..n {
            parent.push(AtomicU32::new(i as u32));
        }
        Self { parent }
    }

    /// Root (== minimum member) of `x`'s component, halving the path walked.
    pub fn find(&self, x: usize) -> usize {
        let mut x = x as u32;
        loop {
            let p = self.parent[x as usize].load(Ordering::Acquire);
            if p == x {
                return x as usize;
            }
            let gp = self.parent[p as usize].load(Ordering::Acquire);
            if gp == p {
                return p as usize;
            }
            let _ = self.parent[x as usize].compare_exchange_weak(
                p,
                gp,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            x = gp;
        }
    }

    /// Unite the components of `a` and `b`; the smaller root wins. Returns
    /// whether the call changed anything (same interface as `UFRush::unite`).
    pub fn unite(&self, a: usize, b: usize) -> bool {
        let mut a = a;
        let mut b = b;
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return false;
            }
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            if self.parent[hi]
                .compare_exchange(hi as u32, lo as u32, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
            a = lo;
            b = hi;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;

    /// Sequential reference: naive DSU with min-relabelled canonical output.
    fn reference_roots(n: usize, edges: &[(usize, usize)]) -> Vec<usize> {
        let mut parent: Vec<usize> = (0..n).collect();
        fn find(parent: &mut Vec<usize>, x: usize) -> usize {
            let mut r = x;
            while parent[r] != r {
                r = parent[r];
            }
            let mut c = x;
            while parent[c] != c {
                let next = parent[c];
                parent[c] = r;
                c = next;
            }
            r
        }
        for &(a, b) in edges {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi] = lo;
            }
        }
        (0..n).map(|i| find(&mut parent, i)).collect()
    }

    fn lcg_edges(n: usize, count: usize, seed: u64) -> Vec<(usize, usize)> {
        let mut state = seed;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        (0..count).map(|_| (next() % n, next() % n)).collect()
    }

    #[test]
    fn root_is_component_minimum() {
        let uf = MinUnionFind::new(200);
        uf.unite(150, 199);
        uf.unite(199, 42);
        uf.unite(90, 42);
        for id in [42usize, 90, 150, 199] {
            assert_eq!(uf.find(id), 42);
        }
        assert_eq!(uf.find(7), 7);
    }

    #[test]
    fn matches_sequential_reference() {
        let n = 5_000;
        let edges = lcg_edges(n, 8_000, 7);
        let uf = MinUnionFind::new(n);
        for &(a, b) in &edges {
            uf.unite(a, b);
        }
        let expect = reference_roots(n, &edges);
        for id in 0..n {
            assert_eq!(uf.find(id), expect[id], "id {id}");
        }
    }

    #[test]
    fn concurrent_unions_match_sequential_reference() {
        // Union-find is order-independent, so a parallel ingest must produce
        // the same partition (and the same min roots) as the serial reference.
        let n = 20_000;
        let edges = lcg_edges(n, 60_000, 99);
        let uf = MinUnionFind::new(n);
        edges.par_iter().for_each(|&(a, b)| {
            uf.unite(a, b);
        });
        let expect = reference_roots(n, &edges);
        for id in 0..n {
            assert_eq!(uf.find(id), expect[id], "id {id}");
        }
    }
}
