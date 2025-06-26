use grammers_tl_types::enums::MessageEntity;
use widestring::{U16Str, U16String};

#[derive(Debug, Clone)]
enum Node {
    Plain(String),
    Entity {
        entity: MessageEntity,
        children: Vec<Node>,
    },
}

impl Node {
    fn to_markdown(&self) -> String {
        fn sanitize(input: &str) -> String {
            let mut result = String::new();

            result.push_str(&"&nbsp;".repeat(input.len() - input.trim_start().len()));
            result.push_str(input.trim());
            result.push_str(&"&nbsp;".repeat(input.len() - input.trim_end().len()));

            result
        }

        match self {
            Node::Plain(s) => s.clone(),
            Node::Entity { entity, children } => {
                let mut inner = String::new();

                for child in children {
                    inner.push_str(&child.to_markdown());
                }

                match entity {
                    MessageEntity::Bold(_) => format!("**{}**", sanitize(&inner)),
                    MessageEntity::Italic(_) => format!("_{}_", sanitize(&inner)),
                    MessageEntity::Strike(_) => format!("~~{}~~", sanitize(&inner)),
                    MessageEntity::Blockquote(_) => format!("> {}", &inner),
                    MessageEntity::Pre(e) => format!("```{}\n{}\n```\n", e.language, inner),
                    MessageEntity::Code(_) => {
                        if inner.find('\n').is_some() {
                            format!("```\n{}\n```", inner)
                        } else {
                            format!("`{}`", inner)
                        }
                    }
                    MessageEntity::MentionName(e) => {
                        format!("[{}](tg://user?id={})", inner, e.user_id)
                    }
                    MessageEntity::TextUrl(e) => format!("[{}]({})", inner, e.url),

                    _ => inner,
                }
            }
        }
    }
}

trait EntityExt {
    fn end(&self) -> usize;
    fn find_children(&self, from: usize, others: &[MessageEntity]) -> Vec<MessageEntity>;
}

impl EntityExt for MessageEntity {
    fn end(&self) -> usize {
        (self.offset() + self.length()) as usize
    }

    fn find_children(&self, from: usize, others: &[MessageEntity]) -> Vec<MessageEntity> {
        let mut found = Vec::new();
        for entity in others.iter().skip(from + 1) {
            if entity.end() > self.end() {
                break;
            }

            found.push(entity.clone());
        }
        found
    }
}

fn tree(msg: &U16Str, entities: &[MessageEntity]) -> Vec<Node> {
    fn parse_children(
        msg: &U16Str,
        entities: &[MessageEntity],
        offset: usize,
        limit: usize,
    ) -> Vec<Node> {
        let mut nodes = Vec::new();
        let mut last = offset;
        let mut i = 0;

        while i < entities.len() {
            let entity = &entities[i];
            let offset = entity.offset() as usize;

            if last < offset {
                nodes.push(Node::Plain(msg[last..offset].to_string_lossy()));
            }

            let children = entity.find_children(i, entities);
            let tree = parse_children(msg, &children, offset, entity.end());

            let node = Node::Entity {
                entity: entity.clone(),
                children: tree,
            };

            last = entity.end();
            nodes.push(node);
            i += children.len() + 1;
        }

        if last < limit && last < msg.len() {
            let text = msg[last..limit.min(msg.len())].to_owned();
            if !text.is_empty() {
                nodes.push(Node::Plain(text.to_string_lossy()));
            }
        }

        nodes
    }

    parse_children(msg, entities, 0, msg.len())
}

pub fn generate_markdown_message(message: &str, entities: &[MessageEntity]) -> String {
    let mut result = String::new();

    for node in tree(&U16String::from_str(message), entities) {
        result.push_str(&node.to_markdown());
    }

    result
}
