use std::cell::RefCell;
use std::rc::Rc;
use std::{
    fs::File,
    io::{prelude::*, BufReader},
    path::Path,
};
use std::dbg;

// A trie (prefix tree) implementation for efficient prefix-based word searches.
// 
// This module provides a trie data structure optimized for storing and retrieving words
// based on their prefixes. The trie is implemented using a vector-based approach where
// each node contains 26 possible child nodes (one for each lowercase letter).
// 
// # Examples
// 
// ```
// use trie::Node;
// 
// let mut trie = Node::new();
// trie::addWord(&mut trie, "apple");
// trie::addWord(&mut trie, "app");
// 
// let matches = trie::prefixMatch(&trie, "app"); // Returns ["app", "apple"]
// ```
// 
// # Implementation Details
// 
// - Each `Node` contains a fixed-size vector of 26 optional child nodes
// - The vector index is calculated by mapping 'a' to 0, 'b' to 1, etc.
// - A boolean flag `endOfWord` marks complete words in the trie
// - The implementation assumes lowercase ASCII characters only
// 
// # Memory Considerations
// 
// The trie uses a vector-based implementation with pre-allocated space for all possible
// child nodes. While this increases memory usage, it provides O(1) access time to child
// nodes compared to using a hash map.

pub struct Node {
    charMap: Vec<Option<Node>>,
    endOfWord: bool,
}

impl Node {
    pub fn new() -> Self {
    let mut node = Node { 
        charMap: Vec::with_capacity(26),
        endOfWord: false
    };
    while node.charMap.len() < 26 {
        node.charMap.push(None);
    }
    node
        // Node { charMap: vec![None; 26] } // Pre-allocate with 26 None elements
    }
}

//on-hold for now; too messy mutable reference rules
//one approach is non mutable iteration to match the word and mutable
//iteration to creates rest of the nodes
// pub fn addWordIter(trie: &mut Node, word: &str) {
//     let mut node = trie;
//     for character in word.chars() {
//         let index = (character as u32 - 97) as usize;

//         // println!("character {} index {}", character, index);
//         let charMap = &mut node.charMap;

//         if let Some(child_node) = &mut charMap[index] {
//             node = child_node;
//             continue;
//         }

//         let new_node = Node::new();
//         charMap[index] = Some(new_node);
        
//         let node_rc = charMap[index].as_mut().unwrap();
//         node = node_rc;
//     }
//     node.endOfWord = true;
// }

pub fn addWord(node: &mut Node, word: &str) {
    if let Some(character) = word.chars().nth(0) {
        let index = (character as u32 - 97) as usize;
        // println!("character {} index {}", character, index);

        if let Some(child_node) = &mut node.charMap[index] {
            let substring = &word[1..];
            addWord(child_node, &substring)
        } else {
            let mut new_node = Node::new();
            node.charMap[index] = Some(new_node);

            let node_rc = node.charMap[index].as_mut().unwrap();

            let substring = &word[1..];
            
            addWord(node_rc, &substring)
        }
    } else {
        node.endOfWord = true;
        return
    }
}

fn findWord(trie: &Node, word: &str) -> bool {
    let mut node = trie;
    for character in word.chars() {
        let index = (character as u32 - 97) as usize;

        if let Some(child_node) = &node.charMap[index] {
            node = child_node;
            continue;
        } else {
            println!("word not found {}", word);
            return false;
        }
    }
    println!("word found {}", word);
    return true;
}

pub fn prefixMatch(trie: &Node, prefix: &str) -> Vec<String> {
    prefixMatchTopK(trie, prefix, usize::MAX)
}

pub fn prefixMatchTopK(trie: &Node, prefix: &str, top_k: usize) -> Vec<String> {
    let mut node = trie;
    for character in prefix.chars() {
        let index = (character as u32 - 97) as usize;

        if let Some(child_node) = &node.charMap[index] {
            node = child_node;
            continue;
        } else {
            println!("prefix not found {}", prefix);
            // return matches;
            return Vec::new();
        }
    }


    //do a dfs traversal starting from node and print all prefixMatches
    let mut matches: Vec<String> = Vec::new();
    if node.endOfWord {
        matches.push(prefix.to_string());
    }

    if matches.len() >= top_k {
        matches.truncate(top_k);
        return matches;
    }

    dfs_top_k(&node, prefix, top_k, &mut matches);
    matches.truncate(top_k);
    matches
}

fn dfs(trie: &Node, pre: &str) -> Vec<String> {
    let mut matches = Vec::new();
    let mut is_last_node = true;

    for index in 0..26 {
        if let Some(child_node) = &trie.charMap[index] {
            is_last_node = false;

            let ascii_code = (index + 97) as u32;
            let character = char::from_u32(ascii_code).unwrap();

            // println!("more character pre {} char {}",pre, character );
            let new_string = format!("{}{}", pre, character);
            let temp_matches = dfs(child_node, &new_string);
            matches.extend_from_slice(&temp_matches);
        } else {

        }
    }

    if is_last_node {
        // println!("completions {}", pre);
        matches.push(pre.to_string());
    }

    return matches;
}

fn dfs_top_k(trie: &Node, pre: &str, top_k: usize, out: &mut Vec<String>) -> bool {
    if out.len() >= top_k {
        return true;
    }

    let mut has_child = false;
    for index in 0..26 {
        if out.len() >= top_k {
            return true;
        }
        if let Some(child_node) = &trie.charMap[index] {
            has_child = true;
            let ascii_code = (index + 97) as u32;
            let character = char::from_u32(ascii_code).unwrap();
            let next = format!("{}{}", pre, character);
            if child_node.endOfWord {
                out.push(next.clone());
                if out.len() >= top_k {
                    return true;
                }
            }
            if dfs_top_k(child_node, &next, top_k, out) {
                return true;
            }
        }
    }

    if !has_child {
        // Leaf node. Historical behavior returned the leaf word; we already emit end-of-word
        // nodes above, so nothing to do here for top_k.
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_match_top_k_is_bounded_and_stable() {
        let mut t = Node::new();
        addWord(&mut t, "app");
        addWord(&mut t, "apple");
        addWord(&mut t, "apply");
        addWord(&mut t, "apt");
        addWord(&mut t, "banana");

        assert_eq!(
            prefixMatchTopK(&t, "app", 1),
            vec!["app".to_string()]
        );
        assert_eq!(
            prefixMatchTopK(&t, "app", 2),
            vec!["app".to_string(), "apple".to_string()]
        );
        assert_eq!(
            prefixMatchTopK(&t, "app", 50),
            vec!["app".to_string(), "apple".to_string(), "apply".to_string()]
        );
        assert_eq!(
            prefixMatchTopK(&t, "ap", 2),
            vec!["app".to_string(), "apple".to_string()]
        );
        assert!(prefixMatchTopK(&t, "zzz", 10).is_empty());
    }
}
