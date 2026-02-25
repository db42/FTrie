use std::{
    fs::File,
    io::{prelude::*, BufReader},
    path::Path,
    collections::HashMap,
};


#[path = "./trie.rs"]
mod trie;

use trie::Node;


pub struct Indexer {
    tenantTrieMap: HashMap<String, Node>,
    //keep tenant trie map
    // trie: Node
}

fn lines_from_file(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}
/// Indexer provides functionality to build and query prefix tries for multiple tenants.
/// Each tenant has its own trie data structure to store and search words.
/// 
/// # Examples
/// 
/// ```
/// let mut indexer = Indexer::new();
/// 
/// // Index words from a file for a specific tenant
/// indexer.indexFile("tenant1", "path/to/wordlist.txt");
/// 
/// // Search for words with a given prefix
/// let matches = indexer.prefixMatch("tenant1", "app"); // Returns words like "apple", "application" etc.
/// ```
/// 
/// The indexer maintains a mapping of tenant IDs to their corresponding tries,
/// allowing for isolated word storage and prefix matching per tenant.

impl Indexer {
    pub fn new() -> Self {
        // let mut trie = Node::new();
        // HashMap::new();
        let indexer = Indexer { tenantTrieMap: HashMap::new() };
        indexer
    }

    pub fn indexFile(&mut self, tenantID: &str, filePath: &str) {
        self.indexFileFiltered(tenantID, filePath, |_| true);
    }

    pub fn indexFileForPrefixRange(
        &mut self,
        tenantID: &str,
        filePath: &str,
        start: char,
        end: char,
    ) {
        let start = start.to_ascii_lowercase();
        let end = end.to_ascii_lowercase();
        self.indexFileFiltered(tenantID, filePath, move |word: &str| {
            let c = match word.chars().next() {
                Some(c) => c.to_ascii_lowercase(),
                None => return false,
            };
            ('a'..='z').contains(&c) && c >= start && c <= end
        });
    }

    fn indexFileFiltered<F>(&mut self, tenantID: &str, filePath: &str, keep: F)
    where
        F: Fn(&str) -> bool,
    {
        // println!("Hello world");

        // let words = lines_from_file("/Users/dushyant.bansal/work/rprojects/helloworld-tonic/words.txt"); //sample
        let words = lines_from_file(filePath); //all words
        // for line in &words {
        //     println!("{:?}", line);
        // }
        // let words: [&str; 3] = ["apple", "april", "mango"];

        //fill trie
        let mut trie = Node::new();

        for word in words.iter() {
            if !keep(word) {
                continue;
            }
            // add word to prefix trie
            // let mut trie_ref = &mut trie;
            // trie::addWord(&mut trie, word);
            // trie::addWordIter(&mut trie, word);
            trie::addWord(&mut trie, word);
        }

        self.tenantTrieMap.insert(tenantID.to_string(), trie);
    }

    pub fn prefixMatch(&self, tenantID: &str, word: &str) -> Vec<String>  {
        //get trie for the tenant
        // let tenantID = "tenant1".to_string();

        if let Some(trie) = self.tenantTrieMap.get(tenantID) {
            // Access the node data here
            return trie::prefixMatch(&trie, word);
        } else {
            return [].to_vec();
        }
    }

    pub fn prefixMatchTopK(&self, tenantID: &str, word: &str, top_k: usize) -> Vec<String> {
        if let Some(trie) = self.tenantTrieMap.get(tenantID) {
            return trie::prefixMatchTopK(trie, word, top_k);
        }
        Vec::new()
    }

    pub fn putWord(&mut self, tenantID: &str, word: &str) {
        let trie = self
            .tenantTrieMap
            .entry(tenantID.to_string())
            .or_insert_with(Node::new);
        trie::addWord(trie, word);
    }
}
