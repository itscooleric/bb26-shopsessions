//! Shopping Sessions: native pipeline (std only, no crates).
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily as Parquet.
//! Follows rules.md byte for byte: only \n ends a line, quotes are syntax around a
//! whole field, empty means null, strings are never normalized.

mod parquet;

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const SHARDS: usize = 512;

// ------------------------------------------------------------------ hashing
#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h: u64 = 0x243F_6A88_85A3_08D3 ^ (b.len() as u64).wrapping_mul(K);
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(K).rotate_left(29);
        i += 8;
    }
    let mut t = 0u64;
    for (k, &c) in b[i..].iter().enumerate() {
        t |= (c as u64) << (8 * k);
    }
    h = (h ^ t).wrapping_mul(K);
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 32)
}

#[inline]
fn hash_bytes2(b: &[u8]) -> u64 {
    const K: u64 = 0xC2B2_AE3D_27D4_EB4F;
    let mut h: u64 = 0x1656_67B1_9E37_79F9 ^ (b.len() as u64).wrapping_mul(K);
    let mut i = 0;
    while i + 8 <= b.len() {
        let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(K).rotate_left(31);
        i += 8;
    }
    let mut t = 0u64;
    for (k, &c) in b[i..].iter().enumerate() {
        t |= (c as u64) << (8 * k);
    }
    h = (h ^ t).wrapping_mul(K);
    h ^= h >> 29;
    h = h.wrapping_mul(0x9FB2_1C65_1E98_DF25);
    h ^ (h >> 32)
}

/// Identity key of an id. Ids of up to 15 bytes are kept inline (their bytes plus the length in the
/// top byte), so equal keys mean equal ids. Longer ids get a 128-bit fingerprint tagged 0xFF in the
/// top byte, and every match on such a key is confirmed byte for byte. (0, 0) is never a key.
type Fp = (u64, u64);
const LONGTAG: u64 = 0xFF << 56;
#[inline]
fn is_long(k: Fp) -> bool {
    k.1 >> 56 == 0xFF
}
#[inline]
fn fp(b: &[u8]) -> Fp {
    let n = b.len();
    if n <= 15 {
        let p = b.as_ptr();
        let (mut w0, mut w1);
        if (p as usize & 4095) <= 4096 - 16 {
            // the 16 bytes stay inside the page that holds the id, which is mapped
            unsafe {
                w0 = (p as *const u64).read_unaligned();
                w1 = (p.add(8) as *const u64).read_unaligned();
            }
        } else {
            let mut t = [0u8; 16];
            t[..n].copy_from_slice(b);
            w0 = u64::from_le_bytes(t[..8].try_into().unwrap());
            w1 = u64::from_le_bytes(t[8..].try_into().unwrap());
        }
        if n < 8 {
            w0 &= (1u64 << (8 * n)) - 1;
            w1 = 0;
        } else if n == 8 {
            w1 = 0;
        } else {
            w1 &= (1u64 << (8 * (n - 8))) - 1;
        }
        (w0, w1 | (n as u64) << 56)
    } else if WEAK_FP.load(Ordering::Relaxed) {
        (n as u64 & 3, LONGTAG) // test mode: long ids collide on purpose, to exercise the byte checks
    } else {
        (hash_bytes(b), (hash_bytes2(b) & !LONGTAG) | LONGTAG)
    }
}
static WEAK_FP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Hash of a key, for table slots and shards.
#[inline]
fn kh(k: Fp) -> u64 {
    #[inline(always)]
    fn fmix(mut h: u64) -> u64 {
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^ (h >> 33)
    }
    fmix(k.0 ^ fmix(k.1))
}

/// Linked id key -> (person name, name key), open addressing, prefetchable.
#[derive(Clone, Copy, Default)]
struct PSlot {
    k: Fp,
    id: Sref,
    name: Sref,
    nf: Fp,
}
struct PTable {
    rbits: u32, // log2 of slots per region; region = top bits of the key hash (the link shard)
    slots: Vec<PSlot>,
}
/// A zero-filled vector without writing it (the kernel hands out zero pages).
fn zeroed_vec<T: Copy>(n: usize) -> Vec<T> {
    if n == 0 || std::mem::size_of::<T>() == 0 {
        return Vec::new();
    }
    let layout = std::alloc::Layout::array::<T>(n).unwrap();
    unsafe {
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        assert!(!p.is_null(), "out of memory");
        Vec::from_raw_parts(p, n, n)
    }
}
const LSH: usize = 256; // link shards
#[inline]
fn lshard(h: u64) -> usize {
    (h >> 56) as usize & (LSH - 1)
}
impl PTable {
    #[inline]
    fn at(&self, h: u64) -> usize {
        (lshard(h) << self.rbits) | (h as usize & ((1 << self.rbits) - 1))
    }
    #[inline]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(self.at(h)) as *const i8, _MM_HINT_T0);
        }
    }
    /// Person of an id with key `k`, hash `h` and bytes `id`, if the id is linked.
    #[inline]
    fn get(&self, k: Fp, h: u64, id: Sref) -> Option<(Sref, Fp)> {
        let m = (1usize << self.rbits) - 1;
        let base = lshard(h) << self.rbits;
        let mut i = h as usize & m;
        loop {
            let s = unsafe { self.slots.get_unchecked(base + i) };
            if s.k == k && (!is_long(k) || s.id.get() == id.get()) {
                return Some((s.name, s.nf));
            }
            if s.k.1 == 0 {
                return None;
            }
            i = (i + 1) & m;
        }
    }
}
/// Insert into one region of a PTable.
fn region_insert(reg: &mut [PSlot], h: u64, k: Fp, id: Sref, name: Sref, nf: Fp) {
    let m = reg.len() - 1;
    let mut i = h as usize & m;
    while reg[i].k.1 != 0 && !(reg[i].k == k && (!is_long(k) || reg[i].id.get() == id.get())) {
        i = (i + 1) & m;
    }
    reg[i] = PSlot { k, id, name, nf };
}

/// Event-id duplicate filter: 2 bits per cell (seen, seen twice), built by a first pass over the events.
/// Ids whose cell says "seen twice" (all real duplicates and a few others) take the exact dedup path.
/// Cells are grouped by event-id shard, so each shard's region is built by one thread.
struct DupCells {
    w: Vec<u64>,
    bits: u32, // log2 of cells per shard
}
impl DupCells {
    #[inline]
    fn cell(&self, h: u64) -> usize {
        (shard_of(h) << self.bits) | (h as usize & ((1 << self.bits) - 1))
    }
    #[inline]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.w.as_ptr().add(self.cell(h) >> 5) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline]
    fn dup(&self, h: u64) -> bool {
        let c = self.cell(h);
        unsafe { self.w.get_unchecked(c >> 5) >> ((c & 31) * 2) & 2 != 0 }
    }
}

/// Records waiting for their person lookup and duplicate check, so cache misses overlap.
const BATCH: usize = 32;
#[inline]
fn flush_ev(pend: &mut Vec<RawEv>, pt: &PTable, dc: &DupCells, direct: &mut [Vec<Ev>], cand: &mut [Vec<RawEv>]) {
    for mut r in pend.drain(..) {
        if r.ok {
            if let Some((nm, nf)) = pt.get(r.pf, kh(r.pf), r.person) {
                r.person = nm;
                r.pf = nf;
            }
        }
        if dc.dup(r.eh) {
            cand[shard_of(r.eh)].push(r);
        } else if r.ok {
            direct[shard_of(kh(r.pf))].push(Ev { ph: r.pf, person: r.person, ts: r.ts, cents: r.cents, cat: r.cat });
        }
    }
}
#[inline]
fn flush_ck(pend: &mut Vec<Ck>, pt: &PTable, local: &mut [Vec<Ck>]) {
    for mut c in pend.drain(..) {
        if let Some((nm, nf)) = pt.get(c.ph, kh(c.ph), c.person) {
            c.person = nm;
            c.ph = nf;
        }
        local[shard_of(kh(c.ph))].push(c);
    }
}

#[derive(Default)]
struct BytesHasher(u64);
impl Hasher for BytesHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = self.0.rotate_left(17) ^ hash_bytes(bytes);
    }
    #[inline]
    fn write_usize(&mut self, _n: usize) {}
    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = self.0.rotate_left(17) ^ n;
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}
type Map<K, V> = HashMap<K, V, BuildHasherDefault<BytesHasher>>;

#[inline]
fn shard_of(h: u64) -> usize {
    ((h >> 40) as usize) & (SHARDS - 1)
}

// ------------------------------------------------------------------ CSV dialect
/// Split a line into exactly N fields; quotes must wrap a whole field. Empty slice = null.
#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut n = 0usize;
    let mut start = 0usize;
    let mut quotes = 0usize; // count of quotes in the current field
    let len = line.len();
    let mut i = 0usize;
    loop {
        let at_end = i == len;
        let c = if at_end { b',' } else { line[i] };
        if c == b',' {
            if n == N {
                return None;
            }
            let f = &line[start..i];
            let v = if quotes == 0 {
                f
            } else {
                if quotes != 2 || f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' {
                    return None;
                }
                &f[1..f.len() - 1]
            };
            out[n] = v;
            n += 1;
            start = i + 1;
            quotes = 0;
            if at_end {
                break;
            }
        } else if c == b'"' {
            quotes += 1;
        }
        i += 1;
    }
    if n != N {
        return None;
    }
    Some(out)
}

/// Call `f` with the N fields of every well-formed line of `d` (same rules as `split_fields`;
/// a trailing piece without \n is a line too). Uses AVX2 when the CPU has it.
#[inline]
fn scan_lines<'a, const N: usize, F: FnMut([&'a [u8]; N])>(d: &'a [u8], f: &mut F) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { scan_avx2::<N, F>(d, f) };
            return;
        }
    }
    for_lines(d, |l| {
        if let Some(x) = split_fields::<N>(l) {
            f(x)
        }
    });
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,popcnt")]
unsafe fn scan_avx2<'a, const N: usize, F: FnMut([&'a [u8]; N])>(d: &'a [u8], f: &mut F) {
    use std::arch::x86_64::*;
    let n = d.len();
    let base = d.as_ptr();
    let vc = _mm256_set1_epi8(b',' as i8);
    let vn = _mm256_set1_epi8(b'\n' as i8);
    let vq = _mm256_set1_epi8(b'"' as i8);
    let mut out: [&'a [u8]; N] = [&[]; N];
    let mut fstart = 0usize;
    let mut qstart = 0u32; // quotes seen before the current field
    let mut nf = 0usize;
    let mut bad = false;
    let mut pos = 0usize;
    let mut qbase = 0u32; // quotes seen before this block
    // close the field [fstart, fe) holding `q` quotes: plain, or wholly wrapped in exactly two quotes
    macro_rules! close {
        ($fe:expr, $q:expr) => {{
            let fe = $fe;
            let q = $q;
            let len = fe - fstart;
            let first = *d.get_unchecked(fstart.min(n - 1));
            let last = *d.get_unchecked(fe.saturating_sub(1));
            let wrapped = (q == 2) & (len >= 2) & (first == b'"') & (last == b'"');
            bad |= (q != 0) & !wrapped;
            let w = wrapped as usize;
            if nf < N {
                *out.get_unchecked_mut(nf) = d.get_unchecked(fstart + w..fe - w);
            } else {
                bad = true;
            }
            nf += 1;
        }};
    }
    let mut pad: [u8; 64];
    while pos < n {
        let p = if pos + 64 <= n {
            base.add(pos)
        } else {
            pad = [0u8; 64];
            std::ptr::copy_nonoverlapping(base.add(pos), pad.as_mut_ptr(), n - pos);
            pad.as_ptr()
        };
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let mc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) as u32 as u64) << 32);
        let mn = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) as u32 as u64) << 32);
        let mq = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) as u32 as u64) << 32);
        let mut s = mc | mn;
        while s != 0 {
            let bit = s & s.wrapping_neg();
            s ^= bit;
            let at = pos + bit.trailing_zeros() as usize;
            let qcur = qbase + (mq & (bit - 1)).count_ones();
            close!(at, qcur - qstart);
            qstart = qcur;
            fstart = at + 1;
            if mn & bit != 0 {
                if !bad && nf == N {
                    f(out);
                }
                nf = 0;
                bad = false;
            }
        }
        qbase += mq.count_ones();
        pos += 64;
    }
    if fstart < n || nf > 0 {
        close!(n, qbase - qstart);
        if !bad && nf == N {
            f(out);
        }
    }
}

// ------------------------------------------------------------------ timestamps
#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        return None;
    }
    Some((a * 10 + c) as i64)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn iso_core(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > md {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

/// Parse a timestamp in F1, F2 or F3. None = bad (or too large to be in any window).
/// Plain reference version: used for the manifest, for lengths the fast path does not cover, and by the self-test.
fn parse_ts_slow(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        if n - i > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in &s[i..] {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    if n == 20 && s[19] == b'Z' {
        return iso_core(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let base = iso_core(s)?;
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    None
}

// ---- fast timestamp parsing (same results as parse_ts_slow for every input; checked by the test `fast_ts_matches_slow`)
const HI8: u64 = 0x8080_8080_8080_8080;
#[inline(always)]
fn ld8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}
/// Days before month m (1..12) in a common year.
const CUM: [u16; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
const MDAYS: [u8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// `YYYY-MM-DDTHH:MM:SS` at the start of `s` (s.len() >= 20), checked 8 bytes at a time: each byte is
/// xored with its expected character ('0' for a digit), so a digit becomes 0..=9 and a separator 0;
/// adding 0x76 (digit) or 0x7F (separator) then sets a byte's top bit exactly when it is wrong.
/// With `zulu`, byte 19 must be 'Z'; without, byte 19 is left to the caller.
#[inline(always)]
fn iso_fast(s: &[u8], zulu: bool) -> Option<i64> {
    const P0: u64 = u64::from_le_bytes(*b"0000-00-");
    const A0: u64 = u64::from_le_bytes([0x76, 0x76, 0x76, 0x76, 0x7F, 0x76, 0x76, 0x7F]);
    const P1: u64 = u64::from_le_bytes(*b"00T00:00");
    const A1: u64 = u64::from_le_bytes([0x76, 0x76, 0x7F, 0x76, 0x76, 0x7F, 0x76, 0x76]);
    const P2Z: u64 = u64::from_le_bytes(*b"0:00:00Z");
    const A2: u64 = u64::from_le_bytes([0x76, 0x7F, 0x76, 0x76, 0x7F, 0x76, 0x76, 0x7F]);
    let t0 = ld8(s, 0) ^ P0;
    let t1 = ld8(s, 8) ^ P1;
    let mut t2 = ld8(s, 12) ^ P2Z;
    if !zulu {
        t2 &= !(0xFFu64 << 56); // byte 19 is the offset sign, checked by the caller
    }
    if ((t0.wrapping_add(A0) | t0) | (t1.wrapping_add(A1) | t1) | (t2.wrapping_add(A2) | t2)) & HI8 != 0 {
        return None;
    }
    let b0 = t0.to_le_bytes();
    let b1 = t1.to_le_bytes();
    let b2 = t2.to_le_bytes();
    let y = b0[0] as u32 * 1000 + b0[1] as u32 * 100 + b0[2] as u32 * 10 + b0[3] as u32;
    let mo = (b0[5] * 10 + b0[6]) as usize;
    let d = (b1[0] * 10 + b1[1]) as u32;
    let h = (b1[3] * 10 + b1[4]) as i64;
    let mi = (b1[6] * 10 + b1[7]) as i64;
    let se = (b2[5] * 10 + b2[6]) as i64;
    if mo.wrapping_sub(1) > 11 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = if mo == 2 { 28 + leap as u32 } else { MDAYS[mo] as u32 };
    if d.wrapping_sub(1) >= md {
        return None;
    }
    // days from 1970-01-01: whole years since year 0 (proleptic Gregorian, year 0 is a leap year), then months
    let yy = y as i64;
    let days = 365 * yy + (yy + 3) / 4 - (yy + 99) / 100 + (yy + 399) / 400 - 719_528
        + CUM[mo] as i64
        + (mo > 2 && leap) as i64
        + d as i64
        - 1;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

/// Value of 8 digit values (0..=9, one per byte, the first byte most significant).
#[inline(always)]
fn swar8(v: u64) -> u64 {
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// Parse a timestamp in F1, F2 or F3. None = bad (or too large to be in any window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        // not all digits, so F1 or F2 or bad
        if n == 20 {
            return iso_fast(s, true);
        }
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let base = iso_fast(s, false)?;
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if sign == b'+' { base - off } else { base + off });
    }
    if n >= 1 && n <= 16 {
        // F3 or bad: up to 16 bytes, all must be digits; leading zeros are just zeros
        let p = s.as_ptr();
        let x: u128 = if (p as usize & 4095) <= 4096 - 16 {
            // the 16 bytes stay inside the page that holds the field, which is mapped
            unsafe { (p as *const u128).read_unaligned() }
        } else {
            let mut t = [0u8; 16];
            t[..n].copy_from_slice(s);
            u128::from_le_bytes(t)
        };
        let keep: u128 = if n == 16 { !0 } else { (1u128 << (8 * n)) - 1 };
        let t = (x ^ 0x3030_3030_3030_3030_3030_3030_3030_3030u128) & keep;
        let bad = (t.wrapping_add(0x7676_7676_7676_7676_7676_7676_7676_7676u128) | t) & 0x8080_8080_8080_8080_8080_8080_8080_8080u128 & keep;
        if bad != 0 {
            return None;
        }
        let a = t << (8 * (16 - n)); // right-align: the missing leading digits are zeros
        let hi = swar8(a as u64); // first 8 digits (most significant)
        let lo = swar8((a >> 64) as u64);
        return Some((hi * 100_000_000 + lo) as i64);
    }
    parse_ts_slow(s)
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in &s[i..] {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ------------------------------------------------------------------ input files
fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// Map every file read-only for the life of the process.
fn map_all(paths: &[PathBuf]) -> Vec<&'static [u8]> {
    use std::os::unix::io::AsRawFd;
    paths
        .iter()
        .map(|p| {
            let f = std::fs::File::open(p).expect("open input");
            let len = f.metadata().expect("stat input").len() as usize;
            if len == 0 {
                return &[][..];
            }
            unsafe {
                let ptr = mmap(std::ptr::null_mut(), len, 1 /*PROT_READ*/, 2 /*MAP_PRIVATE*/, f.as_raw_fd(), 0);
                assert!(ptr as isize != -1, "mmap failed");
                madvise(ptr, len, 2 /*MADV_SEQUENTIAL*/);
                std::slice::from_raw_parts(ptr, len)
            }
        })
        .collect()
}

/// A field of an input file in 64 bits: its address (48 bits) and length (16 bits).
/// Fields of 65,535 bytes or more go to a side table, so no length or file count aborts.
#[derive(Clone, Copy, Default)]
struct Sref(u64);
static LONG: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());
const AMASK: u64 = (1u64 << 48) - 1;
impl Sref {
    #[inline]
    fn new(_file: usize, _base: &[u8], s: &[u8]) -> Sref {
        let p = s.as_ptr() as u64;
        if s.len() < 0xFFFF && p <= AMASK {
            Sref(((s.len() as u64) << 48) | p)
        } else {
            Sref::new_long(s)
        }
    }
    #[cold]
    fn new_long(s: &[u8]) -> Sref {
        let mut l = LONG.lock().unwrap();
        l.push((s.as_ptr() as usize, s.len()));
        Sref((0xFFFFu64 << 48) | (l.len() - 1) as u64)
    }
    #[inline]
    fn get(self) -> &'static [u8] {
        let len = (self.0 >> 48) as usize;
        if len == 0 {
            return &[];
        }
        if len == 0xFFFF {
            return Sref::get_long(self.0 & AMASK);
        }
        unsafe { std::slice::from_raw_parts((self.0 & AMASK) as *const u8, len) }
    }
    #[cold]
    fn get_long(i: u64) -> &'static [u8] {
        let (p, n) = LONG.lock().unwrap()[i as usize];
        unsafe { std::slice::from_raw_parts(p as *const u8, n) }
    }
}

/// Work units: (file index, start, end) byte ranges of whole lines, header excluded.
fn chunks(files: &[&[u8]], target: usize) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for (fi, d) in files.iter().enumerate() {
        let body = match d.iter().position(|&c| c == b'\n') {
            Some(p) => p + 1,
            None => continue, // header only (or empty)
        };
        let mut s = body;
        while s < d.len() {
            let mut e = (s + target).min(d.len());
            if e < d.len() {
                match d[e..].iter().position(|&c| c == b'\n') {
                    Some(p) => e = e + p + 1,
                    None => e = d.len(),
                }
            }
            out.push((fi, s, e));
            s = e;
        }
    }
    out
}

/// Iterate the lines of a byte range: each \n ends a line; a trailing piece without \n is a line too.
#[inline]
fn for_lines<'a>(d: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut s = 0;
    let n = d.len();
    while s < n {
        let e = match memchr_nl(&d[s..]) {
            Some(p) => s + p,
            None => n,
        };
        f(&d[s..e]);
        s = e + 1;
    }
}

/// Call `f` with the first field of every line of `d`, without its wrapping quotes.
fn first_fields<'a>(d: &'a [u8], f: &mut impl FnMut(&'a [u8])) {
    #[inline(always)]
    fn at<'a>(d: &'a [u8], i: usize, f: &mut impl FnMut(&'a [u8])) {
        let mut j = i;
        #[cfg(target_arch = "x86_64")]
        if i + 16 <= d.len() {
            unsafe {
                use std::arch::x86_64::*;
                let v = _mm_loadu_si128(d.as_ptr().add(i) as *const __m128i);
                let m = _mm_movemask_epi8(_mm_or_si128(_mm_cmpeq_epi8(v, _mm_set1_epi8(b',' as i8)), _mm_cmpeq_epi8(v, _mm_set1_epi8(b'\n' as i8))));
                if m != 0 {
                    j = i + m.trailing_zeros() as usize;
                } else {
                    j = i + 16;
                }
            }
        }
        while j < d.len() && d[j] != b',' && d[j] != b'\n' {
            j += 1;
        }
        let mut x = &d[i..j];
        if x.len() >= 2 && x[0] == b'"' && x[x.len() - 1] == b'"' {
            x = &x[1..x.len() - 1];
        }
        f(x);
    }
    if d.is_empty() {
        return;
    }
    at(d, 0, f);
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe { nl_avx2(d, &mut |p| if p + 1 < d.len() { at(d, p + 1, f) }) };
            return;
        }
    }
    let mut i = 0;
    while let Some(p) = memchr_nl(&d[i..]) {
        if i + p + 1 < d.len() {
            at(d, i + p + 1, f);
        }
        i += p + 1;
    }
}

/// Call `f` with the position of every \n in `d`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1")]
unsafe fn nl_avx2(d: &[u8], f: &mut impl FnMut(usize)) {
    use std::arch::x86_64::*;
    let vn = _mm256_set1_epi8(b'\n' as i8);
    let n = d.len();
    let p = d.as_ptr();
    let mut pos = 0;
    while pos + 64 <= n {
        let a = _mm256_loadu_si256(p.add(pos) as *const __m256i);
        let b = _mm256_loadu_si256(p.add(pos + 32) as *const __m256i);
        let mut m = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) as u32 as u64) << 32);
        while m != 0 {
            f(pos + m.trailing_zeros() as usize);
            m &= m - 1;
        }
        pos += 64;
    }
    while pos < n {
        if *p.add(pos) == b'\n' {
            f(pos);
        }
        pos += 1;
    }
}

#[inline]
fn memchr_nl(h: &[u8]) -> Option<usize> {
    // word-at-a-time search for b'\n'
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    const NL: u64 = 0x0a0a_0a0a_0a0a_0a0a;
    let mut i = 0;
    let n = h.len();
    while i + 8 <= n {
        let w = u64::from_le_bytes(h[i..i + 8].try_into().unwrap()) ^ NL;
        let z = w.wrapping_sub(LO) & !w & HI;
        if z != 0 {
            return Some(i + (z.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }
    while i < n {
        if h[i] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Run `f` over work units on `threads` threads, each thread with its own state from `init`.
fn par_units<S: Send, F: Fn(&mut S, usize) + Sync>(n_units: usize, threads: usize, init: impl Fn() -> S + Sync, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n_units {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn read_small_csv(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let d = std::fs::read(path).expect("read small csv");
    let mut out = Vec::new();
    if let Some(p) = d.iter().position(|&c| c == b'\n') {
        for_lines(&d[p + 1..], |l| {
            if let Some([a, b]) = split_fields::<2>(l) {
                if !a.is_empty() {
                    out.push((a.to_vec(), b.to_vec()));
                }
            }
        });
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let k = format!("\"{}\"", key);
    let p = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[p..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = rest[q1..].find('"').unwrap() + q1;
    parse_ts_slow(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

// ------------------------------------------------------------------ records
#[derive(Clone, Copy)]
struct RawEv {
    eh: u64,
    pf: Fp,
    eid: Sref,
    person: Sref,
    ts: i64,
    ing: i64,
    cents: u32, // purchase price; 0 for view/cart
    cat: u16,
    ok: bool, // passes validation and the late check
}

#[derive(Clone, Copy)]
struct Ev {
    ph: Fp,
    person: Sref,
    ts: i64,
    cents: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Ck {
    ph: Fp,
    ts: i64,
    cid: Sref,
    person: Sref,
    camp: u16,
}

struct UserRow {
    person: Sref,
    day: i32,
    sessions: i64,
    events: i64,
    rev: i64,
    top: Option<u16>,
}

type Slots<T> = Vec<std::sync::Mutex<Vec<T>>>;

fn new_slots<T>(threads: usize) -> Slots<T> {
    (0..threads * SHARDS).map(|_| std::sync::Mutex::new(Vec::new())).collect()
}

fn main() {
    let t0 = Instant::now();
    if std::env::var_os("SESSIONS_TEST_WEAK_FP").is_some() {
        WEAK_FP.store(true, Ordering::Relaxed);
    }
    let log = |m: &str| eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), m);
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| std::env::var("DATA_DIR").unwrap_or("/data".into())));
    let out = PathBuf::from(args.get(2).cloned().unwrap_or_else(|| std::env::var("OUTPUT_DIR").unwrap_or("/output".into())));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);

    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");
    let cws = ws - WEEK;

    // products -> category index; categories ranked in byte order, "unknown" included
    let products = read_small_csv(&data.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = products.iter().map(|(_, c)| c.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_idx: Map<&[u8], u16> = cats.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u16)).collect();
    let unknown = cat_idx[b"unknown".as_slice()];
    let prod: Map<&[u8], u16> = products.iter().map(|(p, c)| (p.as_slice(), cat_idx[c.as_slice()])).collect();
    // campaigns -> index; index len() is organic
    let campaigns = read_small_csv(&data.join("campaigns.csv"));
    let camp: Map<&[u8], u16> = campaigns.iter().enumerate().map(|(i, (c, _))| (c.as_slice(), i as u16)).collect();
    let organic = campaigns.len() as u16;
    let ncamp = campaigns.len() + 1;

    let ev_paths = csv_files(&data.join("events"));
    let ck_paths = csv_files(&data.join("clicks"));
    let ln_paths = csv_files(&data.join("links"));
    let mut all = ev_paths.clone();
    all.extend(ck_paths.iter().cloned());
    all.extend(ln_paths.iter().cloned());
    let bases = map_all(&all);
    let ne = ev_paths.len();
    let nc = ck_paths.len();
    let ev_files: Vec<&[u8]> = bases[..ne].to_vec();
    let ck_files: Vec<&[u8]> = bases[ne..ne + nc].to_vec();
    let ln_files: Vec<&[u8]> = bases[ne + nc..].to_vec();
    log("mapped files");

    // ---------------------------------------------------------------- links
    // parse, then put each end of each link into its link shard (by key hash), tagged with where it goes
    let ln_units = chunks(&ln_files, 4 << 20);
    let tid_next = AtomicUsize::new(0);
    type End = (Fp, Sref, u64); // key, bytes, (thread << 40) | (link number * 2 + side)
    let ln_parts = par_units(
        ln_units.len(),
        threads,
        || {
            let t = tid_next.fetch_add(1, Ordering::Relaxed);
            (t, 0u64, (0..LSH).map(|_| Vec::new()).collect::<Vec<Vec<End>>>())
        },
        |st, u| {
            let (fi, s, e) = ln_units[u];
            let base = ln_files[fi];
            let t = (st.0 as u64) << 40;
            scan_lines::<2, _>(&base[s..e], &mut |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    let (ka, kb) = (fp(a), fp(b));
                    let n = st.1;
                    st.2[lshard(kh(ka))].push((ka, Sref::new(0, base, a), t | (n * 2)));
                    st.2[lshard(kh(kb))].push((kb, Sref::new(0, base, b), t | (n * 2 + 1)));
                    st.1 += 1;
                }
            });
        },
    );
    let mut lbase = vec![0u64; threads + 1]; // first link number of each thread
    let mut ln_shards: Vec<Vec<std::sync::Mutex<Vec<End>>>> = (0..LSH).map(|_| Vec::new()).collect();
    for (t, n, v) in ln_parts {
        lbase[t + 1] = n;
        for (sh, x) in v.into_iter().enumerate() {
            ln_shards[sh].push(std::sync::Mutex::new(x));
        }
    }
    for t in 0..threads {
        lbase[t + 1] += lbase[t];
    }
    let nlinks = lbase[threads] as usize;
    log("links: parsed");
    // per link shard: intern the ids (long keys confirmed byte for byte); each link end gets (shard, local id)
    let ends: Vec<u64> = zeroed_vec(nlinks * 2);
    let ends_p = ends.as_ptr() as usize;
    let next = AtomicUsize::new(0);
    let sh_ids: Vec<std::sync::Mutex<Vec<(Sref, Fp)>>> = (0..LSH).map(|_| std::sync::Mutex::new(Vec::new())).collect();
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| {
                let mut slots: Vec<(Fp, u32)> = Vec::new();
                loop {
                    let sh = next.fetch_add(1, Ordering::Relaxed);
                    if sh >= LSH {
                        break;
                    }
                    let parts: Vec<Vec<End>> = ln_shards[sh].iter().map(|m| std::mem::take(&mut *m.lock().unwrap())).collect();
                    let total: usize = parts.iter().map(|p| p.len()).sum();
                    let cap = (total * 2).next_power_of_two().max(16);
                    let mask = cap - 1;
                    slots.clear();
                    slots.resize(cap, ((0, 0), 0));
                    let mut ids: Vec<(Sref, Fp)> = Vec::new();
                    for p in &parts {
                        for &(k, s, tag) in p {
                            let mut i = kh(k) as usize & mask;
                            let x = loop {
                                let sl = slots[i];
                                if sl.0 == k && (!is_long(k) || ids[sl.1 as usize].0.get() == s.get()) {
                                    break sl.1;
                                }
                                if sl.0 .1 == 0 {
                                    let x = ids.len() as u32;
                                    ids.push((s, k));
                                    slots[i] = (k, x);
                                    break x;
                                }
                                i = (i + 1) & mask;
                            };
                            let at = lbase[(tag >> 40) as usize] * 2 + (tag & ((1 << 40) - 1));
                            // SAFETY: every link end is written once, by the thread that owns its shard
                            unsafe { *(ends_p as *mut u64).add(at as usize) = ((sh as u64) << 32) | x as u64 };
                        }
                    }
                    *sh_ids[sh].lock().unwrap() = ids;
                }
            });
        }
    });
    let sh_ids: Vec<Vec<(Sref, Fp)>> = sh_ids.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let mut sbase = vec![0u32; LSH + 1];
    for sh in 0..LSH {
        sbase[sh + 1] = sbase[sh] + sh_ids[sh].len() as u32;
    }
    let nids = sbase[LSH] as usize;
    let gid = |e: u64| sbase[(e >> 32) as usize] + (e as u32);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while p[r as usize] != r {
            r = p[r as usize];
        }
        while p[x as usize] != r {
            let n = p[x as usize];
            p[x as usize] = r;
            x = n;
        }
        r
    }
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    for l in 0..nlinks {
        let ra = find(&mut parent, gid(ends[2 * l]));
        let rb = find(&mut parent, gid(ends[2 * l + 1]));
        if ra != rb {
            parent[rb as usize] = ra;
        }
    }
    drop(ends);
    for i in 0..nids as u32 {
        find(&mut parent, i);
    }
    log("links: union-find");
    // name of each set: the byte-wise smallest id in it (each thread owns the sets whose root % threads is its number)
    let mut name: Vec<u32> = (0..nids as u32).collect();
    {
        let name_p = name.as_mut_ptr() as usize;
        let parent = &parent;
        let sh_ids = &sh_ids;
        let sbase = &sbase;
        std::thread::scope(|sc| {
            for t in 0..threads {
                sc.spawn(move || {
                    let nm = name_p as *mut u32;
                    let mut g = 0u32;
                    for sh in 0..LSH {
                        for &(s, _) in &sh_ids[sh] {
                            let r = parent[g as usize];
                            if r as usize % threads == t && r != g {
                                // SAFETY: only this thread touches name[r] for its roots
                                unsafe {
                                    let cur = *nm.add(r as usize);
                                    let cs = {
                                        let csh = sbase.partition_point(|&b| b <= cur) - 1;
                                        sh_ids[csh][(cur - sbase[csh]) as usize].0
                                    };
                                    if s.get() < cs.get() {
                                        *nm.add(r as usize) = g;
                                    }
                                }
                            }
                            g += 1;
                        }
                    }
                });
            }
        });
    }
    log("links: names");
    // linked id key -> (name, name key), one table region per link shard
    let maxsh = sh_ids.iter().map(|v| v.len()).max().unwrap_or(0);
    let rbits = (maxsh * 2).next_power_of_two().max(4).trailing_zeros();
    let mut pt = PTable { rbits, slots: zeroed_vec(LSH << rbits) };
    {
        let regions: Vec<std::sync::Mutex<&mut [PSlot]>> = pt.slots.chunks_mut(1 << rbits).map(std::sync::Mutex::new).collect();
        let next = AtomicUsize::new(0);
        let (parent, name, sh_ids, sbase) = (&parent, &name, &sh_ids, &sbase);
        std::thread::scope(|sc| {
            for _ in 0..threads {
                sc.spawn(|| loop {
                    let sh = next.fetch_add(1, Ordering::Relaxed);
                    if sh >= LSH {
                        break;
                    }
                    let mut reg = regions[sh].lock().unwrap();
                    for (x, &(s, k)) in sh_ids[sh].iter().enumerate() {
                        let g = sbase[sh] + x as u32;
                        let n = name[parent[g as usize] as usize];
                        let nsh = sbase.partition_point(|&b| b <= n) - 1;
                        let (nm, nf) = sh_ids[nsh][(n - sbase[nsh]) as usize];
                        region_insert(&mut reg, kh(k), k, s, nm, nf);
                    }
                });
            }
        });
    }
    let pt = &pt;
    log(&format!("links: {} ids", nids));

    // ---------------------------------------------------------------- events, pass A: hash every event id, by shard;
    // then per shard mark ids seen once / twice
    let ev_units = chunks(&ev_files, 8 << 20);
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    let ncells = ((ev_bytes / 48).max(1 << 12) * 8).next_power_of_two().max(SHARDS * 64);
    let cbits = (ncells / SHARDS).trailing_zeros();
    let eh_slots: Slots<u64> = new_slots(threads);
    let tid_next = AtomicUsize::new(0);
    par_units(
        ev_units.len(),
        threads,
        || {
            let t = tid_next.fetch_add(1, Ordering::Relaxed);
            (t, (0..SHARDS).map(|_| Vec::new()).collect::<Vec<Vec<u64>>>())
        },
        |st, u| {
            let (fi, s, e) = ev_units[u];
            first_fields(&ev_files[fi][s..e], &mut |f| {
                let h = kh(fp(f));
                st.1[shard_of(h)].push(h);
            });
        },
    )
    .into_iter()
    .for_each(|(t, v)| {
        for (sh, x) in v.into_iter().enumerate() {
            *eh_slots[t * SHARDS + sh].lock().unwrap() = x;
        }
    });
    log("events: ids hashed");
    let mut cell_words: Vec<u64> = vec![0; ncells / 32];
    {
        let per = (1usize << cbits) / 32; // words per shard
        let regions: Vec<std::sync::Mutex<&mut [u64]>> = cell_words.chunks_mut(per).map(std::sync::Mutex::new).collect();
        let next = AtomicUsize::new(0);
        std::thread::scope(|sc| {
            for _ in 0..threads {
                sc.spawn(|| loop {
                    let sh = next.fetch_add(1, Ordering::Relaxed);
                    if sh >= SHARDS {
                        break;
                    }
                    let mut r = regions[sh].lock().unwrap();
                    let m = (1usize << cbits) - 1;
                    for tt in 0..threads {
                        let v = std::mem::take(&mut *eh_slots[tt * SHARDS + sh].lock().unwrap());
                        for h in v {
                            let c = h as usize & m;
                            let w = &mut r[c >> 5];
                            let b = 1u64 << ((c & 31) * 2);
                            *w |= (*w & b) << 1 | b;
                        }
                    }
                });
            }
        });
    }
    drop(eh_slots);
    let dc = DupCells { w: cell_words, bits: cbits };
    let dc = &dc;
    log("events: duplicate filter");

    // ---------------------------------------------------------------- events, pass B: parse + validate; ids seen once go
    // straight to their person's shard, possible duplicates to their event id's shard
    let ev_direct: Slots<Ev> = new_slots(threads);
    let ev_slots: Slots<RawEv> = new_slots(threads);
    let tid_next = AtomicUsize::new(0);
    par_units(
        ev_units.len(),
        threads,
        || {
            let t = tid_next.fetch_add(1, Ordering::Relaxed);
            (t, (0..SHARDS).map(|_| Vec::new()).collect::<Vec<Vec<Ev>>>(), (0..SHARDS).map(|_| Vec::new()).collect::<Vec<Vec<RawEv>>>())
        },
        |st, u| {
            let (fi, s, e) = ev_units[u];
            let base = ev_files[fi];
            let mut pend: Vec<RawEv> = Vec::with_capacity(BATCH);
            scan_lines::<7, _>(&base[s..e], &mut |[eid, uid, ts, ing, et, pid, price]| {
                if eid.is_empty() {
                    return;
                }
                let h = kh(fp(eid));
                dc.prefetch(h);
                let ing_ms = parse_ts(ing).unwrap_or(i64::MAX);
                let tsv = parse_ts(ts);
                let t = tsv.unwrap_or(i64::MIN);
                let mut ok = !uid.is_empty() && !pid.is_empty() && tsv.is_some() && t >= ws && t < we;
                let mut cents = 0u32;
                let mut cat = 0u16;
                if ok {
                    match et {
                        b"view" | b"cart" => {}
                        b"purchase" => match parse_price(price) {
                            Some(p) => {
                                cents = p;
                                cat = *prod.get(pid).unwrap_or(&unknown);
                            }
                            None => ok = false,
                        },
                        _ => ok = false,
                    }
                }
                ok = ok && ing_ms.saturating_sub(t) <= DAY;
                let (pp, pf) = if ok { (Sref::new(fi, base, uid), fp(uid)) } else { (Sref::default(), (0, 0)) };
                if ok {
                    pt.prefetch(kh(pf));
                }
                pend.push(RawEv { eh: h, pf, eid: Sref::new(fi, base, eid), person: pp, ts: t, ing: ing_ms, cents, cat, ok });
                if pend.len() == BATCH {
                    flush_ev(&mut pend, pt, dc, &mut st.1, &mut st.2);
                }
            });
            flush_ev(&mut pend, pt, dc, &mut st.1, &mut st.2);
        },
    )
    .into_iter()
    .for_each(|(t, v, w)| {
        for (sh, x) in v.into_iter().enumerate() {
            *ev_direct[t * SHARDS + sh].lock().unwrap() = x;
        }
        for (sh, x) in w.into_iter().enumerate() {
            *ev_slots[t * SHARDS + sh].lock().unwrap() = x;
        }
    });
    log("events parsed");

    // ---------------------------------------------------------------- clicks: parse + validate, straight to the person's shard
    // (copies of a click are identical, so they meet in the same person and are dropped there)
    let ck_units = chunks(&ck_files, 4 << 20);
    let ck2: Slots<Ck> = new_slots(threads);
    let tid_next = AtomicUsize::new(0);
    par_units(
        ck_units.len(),
        threads,
        || {
            let t = tid_next.fetch_add(1, Ordering::Relaxed);
            (t, (0..SHARDS).map(|_| Vec::new()).collect::<Vec<Vec<Ck>>>())
        },
        |st, u| {
            let (fi, s, e) = ck_units[u];
            let base = ck_files[fi];
            let mut pend: Vec<Ck> = Vec::with_capacity(BATCH);
            scan_lines::<4, _>(&base[s..e], &mut |[cid, uid, cmp, ts]| {
                if cid.is_empty() || uid.is_empty() {
                    return;
                }
                let tsv = parse_ts(ts);
                let t = tsv.unwrap_or(i64::MIN);
                let c = match camp.get(cmp) {
                    Some(&c) if tsv.is_some() && t >= cws && t < we => c,
                    _ => return,
                };
                let pf = fp(uid);
                pt.prefetch(kh(pf));
                pend.push(Ck { ph: pf, ts: t, cid: Sref::new(ne + fi, base, cid), person: Sref::new(ne + fi, base, uid), camp: c });
                if pend.len() == BATCH {
                    flush_ck(&mut pend, pt, &mut st.1);
                }
            });
            flush_ck(&mut pend, pt, &mut st.1);
        },
    )
    .into_iter()
    .for_each(|(t, v)| {
        for (sh, x) in v.into_iter().enumerate() {
            *ck2[t * SHARDS + sh].lock().unwrap() = x;
        }
    });
    log("clicks parsed");

    // ---------------------------------------------------------------- possible duplicates: exact dedup per event-id shard,
    // then to the person's shard
    let ev2: Slots<Ev> = new_slots(threads);
    let shard_next = AtomicUsize::new(0);
    let tid_next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| {
                let t = tid_next.fetch_add(1, Ordering::Relaxed);
                let mut evs: Vec<Vec<Ev>> = (0..SHARDS).map(|_| Vec::new()).collect();
                let mut table: Vec<u64> = Vec::new();
                let mut lose: Vec<bool> = Vec::new();
                loop {
                    let sh = shard_next.fetch_add(1, Ordering::Relaxed);
                    if sh >= SHARDS {
                        break;
                    }
                    // dedup by event_id over this shard's pieces in place: an open-addressing table of
                    // (hash tag, record number); keep the smallest ingest_ts (the earlier record on a tie);
                    // equal hashes are confirmed byte for byte
                    let mut gs: Vec<_> = (0..threads).map(|tt| ev_slots[tt * SHARDS + sh].lock().unwrap()).collect();
                    let total: usize = gs.iter().map(|g| g.len()).sum();
                    let cap = (total * 2).next_power_of_two().max(64);
                    let mask = cap - 1;
                    table.clear();
                    table.resize(cap, u64::MAX);
                    lose.clear();
                    lose.resize(total, false);
                    let get = |gs: &[std::sync::MutexGuard<Vec<RawEv>>], gi: u32| -> RawEv {
                        let mut g = gi as usize;
                        for v in gs {
                            if g < v.len() {
                                return v[g];
                            }
                            g -= v.len();
                        }
                        unreachable!()
                    };
                    let mut gi = 0u32;
                    for v in gs.iter() {
                        for r in v.iter() {
                            let tag = r.eh >> 32;
                            let mut slot = r.eh as usize & mask;
                            loop {
                                let e = table[slot];
                                if e == u64::MAX {
                                    table[slot] = (tag << 32) | gi as u64;
                                    break;
                                }
                                if e >> 32 == tag {
                                    let oi = e as u32;
                                    let o = get(&gs, oi);
                                    if o.eh == r.eh && o.eid.get() == r.eid.get() {
                                        if r.ing < o.ing {
                                            lose[oi as usize] = true;
                                            table[slot] = (tag << 32) | gi as u64;
                                        } else {
                                            lose[gi as usize] = true;
                                        }
                                        break;
                                    }
                                }
                                slot = (slot + 1) & mask;
                            }
                            gi += 1;
                        }
                    }
                    let mut gi = 0usize;
                    for v in gs.iter_mut() {
                        for r in v.iter() {
                            if r.ok && !lose[gi] {
                                evs[shard_of(kh(r.pf))].push(Ev { ph: r.pf, person: r.person, ts: r.ts, cents: r.cents, cat: r.cat });
                            }
                            gi += 1;
                        }
                        **v = Vec::new();
                    }
                }
                for (sh, x) in evs.into_iter().enumerate() {
                    *ev2[t * SHARDS + sh].lock().unwrap() = x;
                }
            });
        }
    });
    drop(ev_slots);
    log("deduped");

    // ---------------------------------------------------------------- pass 3: per person shard: sessions, credit, assists
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncat = cats.len();
    let shard_next = AtomicUsize::new(0);
    struct Acc {
        users: Vec<UserRow>,
        cr: Vec<[i64; 3]>, // per camp*ndays+day: sessions, purchasing, revenue
        asx: Vec<i64>,     // per camp*ndays+day: assisted sessions
    }
    let accs: Vec<Acc> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(|| {
                    let mut acc = Acc { users: Vec::new(), cr: vec![[0; 3]; ncamp * ndays], asx: vec![0; ncamp * ndays] };
                    let mut evs: Vec<Ev> = Vec::new();
                    let mut cks: Vec<Ck> = Vec::new();
                    let mut starts: Vec<i64> = Vec::new();
                    let mut catsum: Vec<i64> = vec![0; ncat];
                    let mut by_camp: Vec<(u16, i64)> = Vec::new();
                    let mut ptab: Vec<(Fp, u32)> = Vec::new();
                    let mut rep: Vec<Sref> = Vec::new();
                    let (mut ecount, mut ccount, mut eoff, mut coff, mut epid, mut cpid): (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>) =
                        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
                    loop {
                        let sh = shard_next.fetch_add(1, Ordering::Relaxed);
                        if sh >= SHARDS {
                            break;
                        }
                        // group this shard's events and clicks by person (counting sort), then sort each person's by time
                        let mut eg: Vec<_> = (0..threads)
                            .map(|tt| ev2[tt * SHARDS + sh].lock().unwrap())
                            .chain((0..threads).map(|tt| ev_direct[tt * SHARDS + sh].lock().unwrap()))
                            .collect();
                        let mut cg: Vec<_> = (0..threads).map(|tt| ck2[tt * SHARDS + sh].lock().unwrap()).collect();
                        let ne_: usize = eg.iter().map(|g| g.len()).sum();
                        let cap = (ne_ * 2).next_power_of_two().max(64);
                        let mask = cap - 1;
                        ptab.clear();
                        ptab.resize(cap, ((0, 0), u32::MAX));
                        ecount.clear();
                        epid.clear();
                        rep.clear();
                        let mut collide = false;
                        for v in eg.iter() {
                            for e in v.iter() {
                                let mut slot = kh(e.ph) as usize & mask;
                                let p = loop {
                                    let (k, p) = ptab[slot];
                                    if p == u32::MAX {
                                        let p = ecount.len() as u32;
                                        ptab[slot] = (e.ph, p);
                                        ecount.push(0);
                                        rep.push(e.person);
                                        break p;
                                    }
                                    if k == e.ph {
                                        if is_long(k) && rep[p as usize].get() != e.person.get() {
                                            collide = true;
                                        }
                                        break p;
                                    }
                                    slot = (slot + 1) & mask;
                                };
                                ecount[p as usize] += 1;
                                epid.push(p);
                            }
                        }
                        let np = ecount.len();
                        ccount.clear();
                        ccount.resize(np, 0);
                        cpid.clear();
                        for v in cg.iter() {
                            for c in v.iter() {
                                let mut slot = kh(c.ph) as usize & mask;
                                let p = loop {
                                    let (k, p) = ptab[slot];
                                    if p == u32::MAX {
                                        break u32::MAX; // a person with no events: their clicks credit nothing
                                    }
                                    if k == c.ph {
                                        if is_long(k) && rep[p as usize].get() != c.person.get() {
                                            collide = true;
                                        }
                                        break p;
                                    }
                                    slot = (slot + 1) & mask;
                                };
                                if p != u32::MAX {
                                    ccount[p as usize] += 1;
                                }
                                cpid.push(p);
                            }
                        }
                        if collide {
                            // two long ids share a fingerprint: group this shard again by the ids' bytes
                            let mut m: HashMap<&[u8], u32> = HashMap::new();
                            ecount.clear();
                            epid.clear();
                            for v in eg.iter() {
                                for e in v.iter() {
                                    let n = m.len() as u32;
                                    let p = *m.entry(e.person.get()).or_insert(n);
                                    if p as usize == ecount.len() {
                                        ecount.push(0);
                                    }
                                    ecount[p as usize] += 1;
                                    epid.push(p);
                                }
                            }
                            ccount.clear();
                            ccount.resize(ecount.len(), 0);
                            cpid.clear();
                            for v in cg.iter() {
                                for c in v.iter() {
                                    let p = m.get(c.person.get()).copied().unwrap_or(u32::MAX);
                                    if p != u32::MAX {
                                        ccount[p as usize] += 1;
                                    }
                                    cpid.push(p);
                                }
                            }
                        }
                        let np = ecount.len();
                        // offsets
                        eoff.clear();
                        coff.clear();
                        let (mut se, mut sc_) = (0u32, 0u32);
                        for p in 0..np {
                            eoff.push(se);
                            coff.push(sc_);
                            se += ecount[p];
                            sc_ += ccount[p];
                        }
                        eoff.push(se);
                        coff.push(sc_);
                        evs.clear();
                        evs.resize(se as usize, Ev { ph: (0, 0), person: Sref::default(), ts: 0, cents: 0, cat: 0 });
                        cks.clear();
                        cks.resize(sc_ as usize, Ck { ph: (0, 0), ts: 0, cid: Sref::default(), person: Sref::default(), camp: 0 });
                        let mut k = 0usize;
                        for v in eg.iter_mut() {
                            for e in v.iter() {
                                let p = epid[k] as usize;
                                let at = eoff[p] + ecount[p] - 1;
                                ecount[p] -= 1;
                                evs[at as usize] = *e;
                                k += 1;
                            }
                            **v = Vec::new();
                        }
                        let mut k = 0usize;
                        for v in cg.iter_mut() {
                            for c in v.iter() {
                                let p = cpid[k];
                                if p != u32::MAX {
                                    let p = p as usize;
                                    let at = coff[p] + ccount[p] - 1;
                                    ccount[p] -= 1;
                                    cks[at as usize] = *c;
                                }
                                k += 1;
                            }
                            **v = Vec::new();
                        }
                        drop(eg);
                        drop(cg);
                        for p in 0..np {
                            let pe = &mut evs[eoff[p] as usize..eoff[p + 1] as usize];
                            if pe.len() <= 24 {
                                // insertion sort: most people have a handful of events, often already in order
                                for x in 1..pe.len() {
                                    let v = pe[x];
                                    let mut y = x;
                                    while y > 0 && pe[y - 1].ts > v.ts {
                                        pe[y] = pe[y - 1];
                                        y -= 1;
                                    }
                                    pe[y] = v;
                                }
                            } else {
                                pe.sort_unstable_by_key(|e| e.ts);
                            }
                            let pc = &mut cks[coff[p] as usize..coff[p + 1] as usize];
                            let mut m = pc.len();
                            if pc.len() > 1 {
                                let less = |a: &Ck, b: &Ck| a.ts < b.ts || (a.ts == b.ts && a.cid.get() < b.cid.get());
                                if pc.len() <= 24 {
                                    for x in 1..pc.len() {
                                        let v = pc[x];
                                        let mut y = x;
                                        while y > 0 && less(&v, &pc[y - 1]) {
                                            pc[y] = pc[y - 1];
                                            y -= 1;
                                        }
                                        pc[y] = v;
                                    }
                                } else {
                                    pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.get().cmp(b.cid.get())));
                                }
                                // copies of a click sit next to each other now: keep one
                                m = 1;
                                for x in 1..pc.len() {
                                    if !(pc[x].ts == pc[m - 1].ts && pc[x].cid.get() == pc[m - 1].cid.get()) {
                                        pc[m] = pc[x];
                                        m += 1;
                                    }
                                }
                            }
                            ccount[p] = m as u32;
                        }
                        for p in 0..np {
                            let pc = &cks[coff[p] as usize..(coff[p] + ccount[p]) as usize];
                            let pe = &evs[eoff[p] as usize..eoff[p + 1] as usize];
                            let person = pe[0].person;
                            starts.clear();
                            let mut cur_day: i64 = i64::MIN;
                            let mut urow = UserRow { person, day: 0, sessions: 0, events: 0, rev: 0, top: None };
                            let mut any_purchase = false;
                            let mut ci = 0usize; // clicks with ts <= start
                            let mut a = 0;
                            while a < pe.len() {
                                let start = pe[a].ts;
                                let mut b = a + 1;
                                while b < pe.len() && pe[b].ts - pe[b - 1].ts <= GAP {
                                    b += 1;
                                }
                                starts.push(start);
                                let mut rev = 0i64;
                                let mut np = 0i64;
                                for e in &pe[a..b] {
                                    if e.cents > 0 {
                                        np += 1;
                                        rev += e.cents as i64;
                                    }
                                }
                                let day = start.div_euclid(DAY);
                                if day != cur_day {
                                    if cur_day != i64::MIN {
                                        if any_purchase {
                                            urow.top = Some(top_cat(&mut catsum));
                                        }
                                        acc.users.push(std::mem::replace(
                                            &mut urow,
                                            UserRow { person, day: 0, sessions: 0, events: 0, rev: 0, top: None },
                                        ));
                                    }
                                    cur_day = day;
                                    urow.day = day as i32;
                                    any_purchase = false;
                                }
                                urow.sessions += 1;
                                urow.events += (b - a) as i64;
                                urow.rev += rev;
                                if np > 0 {
                                    any_purchase = true;
                                    for e in &pe[a..b] {
                                        if e.cents > 0 {
                                            catsum[e.cat as usize] += e.cents as i64;
                                        }
                                    }
                                }
                                // credit: latest click at or before the start, within 7 days; ties to smallest click_id
                                while ci < pc.len() && pc[ci].ts <= start {
                                    ci += 1;
                                }
                                let mut credited = organic;
                                if ci > 0 {
                                    let top_ts = pc[ci - 1].ts;
                                    if start - top_ts <= WEEK {
                                        let mut f = ci - 1;
                                        while f > 0 && pc[f - 1].ts == top_ts {
                                            f -= 1;
                                        }
                                        credited = pc[f].camp;
                                    }
                                }
                                let cell = &mut acc.cr[credited as usize * ndays + (day - day0) as usize];
                                cell[0] += 1;
                                cell[1] += (np > 0) as i64;
                                cell[2] += rev;
                                a = b;
                            }
                            if cur_day != i64::MIN {
                                if any_purchase {
                                    urow.top = Some(top_cat(&mut catsum));
                                }
                                acc.users.push(urow);
                            }
                            // assists: each click covers [ts, next click of the same campaign - 1], capped at ts + 7 days
                            if !pc.is_empty() {
                                by_camp.clear();
                                by_camp.extend(pc.iter().map(|c| (c.camp, c.ts)));
                                by_camp.sort_unstable();
                                for x in 0..by_camp.len() {
                                    let (cm, lo) = by_camp[x];
                                    let mut hi = lo + WEEK;
                                    if x + 1 < by_camp.len() && by_camp[x + 1].0 == cm {
                                        hi = hi.min(by_camp[x + 1].1 - 1);
                                    }
                                    if hi < lo {
                                        continue;
                                    }
                                    let mut p0 = starts.partition_point(|&s| s < lo);
                                    while p0 < starts.len() && starts[p0] <= hi {
                                        let d = starts[p0].div_euclid(DAY);
                                        let dend = ((d + 1) * DAY - 1).min(hi);
                                        let p1 = p0 + starts[p0..].partition_point(|&s| s <= dend);
                                        acc.asx[cm as usize * ndays + (d - day0) as usize] += (p1 - p0) as i64;
                                        p0 = p1;
                                    }
                                }
                            }
                        }
                    }
                    acc
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    log("sessions + attribution");

    // ---------------------------------------------------------------- outputs
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let mut cr = vec![[0i64; 3]; ncamp * ndays];
    let mut asx = vec![0i64; ncamp * ndays];
    for a in &accs {
        for (x, y) in cr.iter_mut().zip(&a.cr) {
            x[0] += y[0];
            x[1] += y[1];
            x[2] += y[2];
        }
        for (x, y) in asx.iter_mut().zip(&a.asx) {
            *x += *y;
        }
    }
    std::thread::scope(|sc| {
        for (ti, a) in accs.iter().enumerate() {
            let ud = &ud;
            let cats = &cats;
            sc.spawn(move || {
                let mut w = parquet::Table::new();
                w.str_col("user_id", false, a.users.iter().map(|u| Some(u.person.get())));
                w.date_col("day", a.users.iter().map(|u| u.day));
                w.i64_col("sessions", a.users.iter().map(|u| u.sessions));
                w.i64_col("events", a.users.iter().map(|u| u.events));
                w.i64_col("revenue_cents", a.users.iter().map(|u| u.rev));
                w.str_col("top_category", true, a.users.iter().map(|u| u.top.map(|c| cats[c as usize].as_slice())));
                w.write(&ud.join(format!("part-{:03}.parquet", ti)), a.users.len());
            });
        }
        let mut rows: Vec<(usize, usize)> = Vec::new();
        for c in 0..ncamp {
            for d in 0..ndays {
                let k = c * ndays + d;
                if cr[k][0] > 0 || asx[k] > 0 {
                    rows.push((c, d));
                }
            }
        }
        let cname = |c: usize| -> &[u8] { if c == organic as usize { b"organic" } else { &campaigns[c].0 } };
        let chan = |c: usize| -> &[u8] { if c == organic as usize { b"organic" } else { &campaigns[c].1 } };
        let mut w = parquet::Table::new();
        w.str_col("campaign_id", false, rows.iter().map(|&(c, _)| Some(cname(c))));
        w.str_col("channel", true, rows.iter().map(|&(c, _)| Some(chan(c)).filter(|s| !s.is_empty())));
        w.date_col("day", rows.iter().map(|&(_, d)| (day0 + d as i64) as i32));
        w.i64_col("sessions", rows.iter().map(|&(c, d)| cr[c * ndays + d][0]));
        w.i64_col("purchasing_sessions", rows.iter().map(|&(c, d)| cr[c * ndays + d][1]));
        w.i64_col("revenue_cents", rows.iter().map(|&(c, d)| cr[c * ndays + d][2]));
        w.i64_col("assisted_sessions", rows.iter().map(|&(c, d)| asx[c * ndays + d]));
        w.write(&cd.join("part-000.parquet"), rows.len());
    });
    log("written");
    // skip freeing gigabytes of buffers one by one: the process ends here and the kernel reclaims it all
    std::process::exit(0);
}

/// Category with the highest summed price; ties to the byte-wise smallest (categories are in byte order). Resets the sums.
fn top_cat(catsum: &mut [i64]) -> u16 {
    let mut bi = 0usize;
    for (c, &v) in catsum.iter().enumerate() {
        if v > catsum[bi] {
            bi = c;
        }
    }
    catsum.iter_mut().for_each(|x| *x = 0);
    bi as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    struct R(u64);
    impl R {
        fn n(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn r(&mut self, k: u64) -> u64 {
            self.n() % k
        }
    }
    fn check(s: &[u8], cnt: &mut [u64; 2]) {
        // also at a page edge, so the copy path is exercised
        let a = parse_ts_slow(s);
        assert_eq!(parse_ts(s), a, "{:?}", String::from_utf8_lossy(s));
        let mut buf = vec![0u8; 3 * 4096];
        let base = buf.as_ptr() as usize;
        let off = (4096 - (base & 4095)) + 4096 - s.len().min(4096); // field ends exactly at a page end
        buf[off..off + s.len()].copy_from_slice(s);
        assert_eq!(parse_ts(&buf[off..off + s.len()]), a, "edge {:?}", String::from_utf8_lossy(s));
        let off2 = (4096 - (base & 4095)) + 4090;
        if off2 + s.len() <= buf.len() {
            buf[off2..off2 + s.len()].copy_from_slice(s);
            assert_eq!(parse_ts(&buf[off2..off2 + s.len()]), a);
        }
        cnt[a.is_some() as usize] += 1;
    }
    #[test]
    fn fast_ts_matches_slow() {
        let mut cnt = [0u64; 2];
        let mut rng = R(0x9E37_79B9_7F4A_7C15);
        let years: Vec<u32> = vec![0, 1, 3, 4, 96, 99, 100, 101, 399, 400, 401, 1600, 1700, 1899, 1900, 1969, 1970, 1971, 1999, 2000, 2001, 2024, 2025, 2026, 2027, 2028, 2029, 2100, 2400, 9999];
        // every valid calendar day of these years, and the invalid neighbours
        for &y in &years {
            for m in 0..=13u32 {
                for d in 0..=32u32 {
                    let f1 = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, rng.r(25), rng.r(61), rng.r(61));
                    check(f1.as_bytes(), &mut cnt);
                    let f2 = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, m, d, rng.r(25), rng.r(61), rng.r(61), if rng.r(2) == 0 { '+' } else { '-' }, rng.r(26), rng.r(62));
                    check(f2.as_bytes(), &mut cnt);
                }
            }
        }
        // days_from_civil agreement over all years 0..=9999, first and last day of every month
        for y in 0..=9999u32 {
            for m in 1..=12u32 {
                for d in [1u32, 28, 29, 30, 31] {
                    let f1 = format!("{:04}-{:02}-{:02}T23:59:59Z", y, m, d);
                    check(f1.as_bytes(), &mut cnt);
                }
            }
        }
        // F3 digit strings, with leading zeros, 1..=25 bytes
        for _ in 0..300_000 {
            let len = 1 + rng.r(25) as usize;
            let mut v: Vec<u8> = (0..len).map(|_| b'0' + rng.r(10) as u8).collect();
            if rng.r(3) == 0 {
                let z = rng.r(len as u64 + 1) as usize;
                for c in v.iter_mut().take(z) {
                    *c = b'0';
                }
            }
            check(&v, &mut cnt);
            // one byte changed to something else
            let alphabet: &[u8] = b"0123456789-:+TZtz. \r\"e/\x00\x7f\x80\xff\xd9\xa2\xef\xbc\x92\x2f\x3a\x30\x39";
            let i = rng.r(len as u64) as usize;
            v[i] = alphabet[rng.r(alphabet.len() as u64) as usize];
            check(&v, &mut cnt);
        }
        // mutations of valid F1 / F2 strings: one or two bytes changed, truncated or extended
        let alphabet: &[u8] = b"0123456789-:+TZtz. \r\"e/\x00\x7f\x80\xff\xd9\xa2\x2f\x3a\x30\x39";
        for _ in 0..1_000_000 {
            let y = years[rng.r(years.len() as u64) as usize];
            let base = if rng.r(2) == 0 {
                format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, 1 + rng.r(12), 1 + rng.r(31), rng.r(24), rng.r(60), rng.r(60))
            } else {
                format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, 1 + rng.r(12), 1 + rng.r(31), rng.r(24), rng.r(60), rng.r(60), if rng.r(2) == 0 { '+' } else { '-' }, rng.r(24), rng.r(60))
            };
            let mut v = base.into_bytes();
            check(&v, &mut cnt);
            for _ in 0..1 + rng.r(2) {
                let i = rng.r(v.len() as u64) as usize;
                v[i] = alphabet[rng.r(alphabet.len() as u64) as usize];
            }
            check(&v, &mut cnt);
            match rng.r(4) {
                0 => {
                    v.truncate(rng.r(v.len() as u64) as usize);
                }
                1 => v.push(alphabet[rng.r(alphabet.len() as u64) as usize]),
                _ => {}
            }
            check(&v, &mut cnt);
        }
        check(b"", &mut cnt);
        check(b"0001791071890000", &mut cnt);
        check(b"1791071890000", &mut cnt);
        check(b"1791071890000.0", &mut cnt);
        check(b"9223372036854775807", &mut cnt);
        check(b"99999999999999999999", &mut cnt);
        check(b"2026-10-03T19:58:10-00:00", &mut cnt);
        check(b"2026-10-03T19:58:10+0200", &mut cnt);
        check(b"2026-10-03T19:58:10z", &mut cnt);
        check(b"2026-10-03 19:58:10Z", &mut cnt);
        check(b"2028-02-29T00:00:00Z", &mut cnt);
        check(b"2027-02-29T00:00:00Z", &mut cnt);
        eprintln!("checked: {} bad, {} good", cnt[0], cnt[1]);
        assert!(cnt[0] > 100_000 && cnt[1] > 100_000);
    }
}
