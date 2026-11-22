//! Minimal Parquet writer: one row group, one PLAIN uncompressed v1 data page per column.
//! Types used: BYTE_ARRAY (UTF8/STRING), INT32 (DATE), INT64. Optional columns use
//! bit-packed definition levels (max level 1).

use std::io::Write;
use std::path::Path;

struct Col {
    name: String,
    ptype: i32,              // 1 INT32, 2 INT64, 6 BYTE_ARRAY
    converted: Option<i32>,  // 0 UTF8, 6 DATE
    logical: Option<i16>,    // union field id: 1 STRING, 6 DATE
    optional: bool,
    page: Vec<u8>,           // page body (def levels + values)
    nulls: usize,
}

pub struct Table {
    cols: Vec<Col>,
}

// ------------------------------------------------------------------ thrift compact protocol
struct Tw {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Tw {
    fn new() -> Self {
        Tw { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.b.push(byte);
                break;
            }
            self.b.push(byte | 0x80);
        }
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i16, t: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | t);
        } else {
            self.b.push(t);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.fh(id, 5);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.fh(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.fh(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin_struct(&mut self, id: i16) {
        self.fh(id, 12);
        self.last.push(0);
    }
    fn begin_list(&mut self, id: i16, elem: u8, n: usize) {
        self.fh(id, 9);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xf0 | elem);
            self.varint(n as u64);
        }
    }
    fn begin_elem_struct(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
}

fn def_levels(present: &[bool]) -> Vec<u8> {
    // RLE/bit-packed hybrid, one bit-packed run, bit width 1, with the v1 4-byte length prefix
    let groups = present.len().div_ceil(8);
    let mut run = Vec::new();
    let mut h = ((groups as u64) << 1) | 1;
    loop {
        let byte = (h & 0x7f) as u8;
        h >>= 7;
        if h == 0 {
            run.push(byte);
            break;
        }
        run.push(byte | 0x80);
    }
    let mut bits = vec![0u8; groups];
    for (i, &p) in present.iter().enumerate() {
        if p {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    run.extend_from_slice(&bits);
    let mut out = (run.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&run);
    out
}

impl Table {
    pub fn new() -> Self {
        Table { cols: Vec::new() }
    }

    pub fn str_col<'a>(&mut self, name: &str, optional: bool, vals: impl Iterator<Item = Option<&'a [u8]>>) {
        let mut present = Vec::new();
        let mut data = Vec::new();
        let mut nulls = 0;
        for v in vals {
            match v {
                Some(s) => {
                    present.push(true);
                    data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    data.extend_from_slice(s);
                }
                None => {
                    assert!(optional, "null in required column {name}");
                    present.push(false);
                    nulls += 1;
                }
            }
        }
        let mut page = if optional { def_levels(&present) } else { Vec::new() };
        page.extend_from_slice(&data);
        self.cols.push(Col { name: name.into(), ptype: 6, converted: Some(0), logical: Some(1), optional, page, nulls });
    }

    pub fn date_col(&mut self, name: &str, vals: impl Iterator<Item = i32>) {
        let mut page = Vec::new();
        for v in vals {
            page.extend_from_slice(&v.to_le_bytes());
        }
        self.cols.push(Col { name: name.into(), ptype: 1, converted: Some(6), logical: Some(6), optional: false, page, nulls: 0 });
    }

    pub fn i64_col(&mut self, name: &str, vals: impl Iterator<Item = i64>) {
        let mut page = Vec::new();
        for v in vals {
            page.extend_from_slice(&v.to_le_bytes());
        }
        self.cols.push(Col { name: name.into(), ptype: 2, converted: None, logical: None, optional: false, page, nulls: 0 });
    }

    pub fn write(&self, path: &Path, nrows: usize) {
        let mut f: Vec<u8> = b"PAR1".to_vec();
        let mut chunks = Vec::new(); // (page header offset, total size)
        for c in &self.cols {
            let off = f.len();
            let mut h = Tw::new();
            h.i32f(1, 0); // DATA_PAGE
            h.i32f(2, c.page.len() as i32);
            h.i32f(3, c.page.len() as i32);
            h.begin_struct(5);
            h.i32f(1, nrows as i32);
            h.i32f(2, 0); // PLAIN
            h.i32f(3, 3); // RLE
            h.i32f(4, 3); // RLE
            h.end_struct();
            h.b.push(0); // end PageHeader
            f.extend_from_slice(&h.b);
            f.extend_from_slice(&c.page);
            chunks.push((off, f.len() - off));
        }
        // FileMetaData
        let mut m = Tw::new();
        m.i32f(1, 1);
        m.begin_list(2, 12, self.cols.len() + 1);
        m.begin_elem_struct();
        m.strf(4, b"schema");
        m.i32f(5, self.cols.len() as i32);
        m.end_struct();
        for c in &self.cols {
            m.begin_elem_struct();
            m.i32f(1, c.ptype);
            m.i32f(3, if c.optional { 1 } else { 0 });
            m.strf(4, c.name.as_bytes());
            if let Some(cv) = c.converted {
                m.i32f(6, cv);
            }
            if let Some(lt) = c.logical {
                m.begin_struct(10);
                m.begin_struct(lt);
                m.end_struct();
                m.end_struct();
            }
            m.end_struct();
        }
        m.i64f(3, nrows as i64);
        m.begin_list(4, 12, 1);
        m.begin_elem_struct();
        m.begin_list(1, 12, self.cols.len());
        let mut total = 0i64;
        for (c, &(off, size)) in self.cols.iter().zip(&chunks) {
            total += size as i64;
            m.begin_elem_struct();
            m.i64f(2, off as i64);
            m.begin_struct(3);
            m.i32f(1, c.ptype);
            m.begin_list(2, 5, 2);
            m.zz(0); // PLAIN
            m.zz(3); // RLE
            m.begin_list(3, 8, 1);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32f(4, 0); // UNCOMPRESSED
            m.i64f(5, nrows as i64);
            m.i64f(6, size as i64);
            m.i64f(7, size as i64);
            m.i64f(9, off as i64);
            m.end_struct();
            m.end_struct();
            let _ = c.nulls;
        }
        m.i64f(2, total);
        m.i64f(3, nrows as i64);
        m.end_struct();
        m.strf(6, b"shopping-sessions");
        m.b.push(0);
        f.extend_from_slice(&m.b);
        f.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
        f.extend_from_slice(b"PAR1");
        let mut file = std::fs::File::create(path).expect("create parquet");
        file.write_all(&f).expect("write parquet");
    }
}
