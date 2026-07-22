//! `CoT` event construction and conservative XML stream framing.

use quick_xml::Reader;
use quick_xml::events::Event;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct PositionEvent {
    pub uid: String,
    pub callsign: String,
    pub latitude: f64,
    pub longitude: f64,
    pub altitude_m: f64,
    pub course_deg: f64,
    pub speed_mps: f64,
    pub time: OffsetDateTime,
    pub stale: OffsetDateTime,
    pub correlation_id: Uuid,
}

impl PositionEvent {
    #[must_use]
    pub fn to_xml(&self) -> String {
        format!(
            concat!(
                "<event version=\"2.0\" uid=\"{}\" type=\"a-f-G-U-C\" ",
                "time=\"{}\" start=\"{}\" stale=\"{}\" how=\"m-g\">",
                "<point lat=\"{}\" lon=\"{}\" hae=\"{}\" ce=\"9999999\" le=\"9999999\"/>",
                "<detail><contact callsign=\"{}\"/><takv device=\"tak_bench\" os=\"Linux\" ",
                "platform=\"tak_bench\" version=\"{}\"/><track course=\"{}\" speed=\"{}\"/>",
                "<tak_bench correlation_id=\"{}\"/></detail></event>"
            ),
            escape(&self.uid),
            timestamp(self.time),
            timestamp(self.time),
            timestamp(self.stale),
            self.latitude,
            self.longitude,
            self.altitude_m,
            escape(&self.callsign),
            env!("CARGO_PKG_VERSION"),
            self.course_deg,
            self.speed_mps,
            self.correlation_id,
        )
    }
}

fn timestamp(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .expect("Rfc3339 formatting is supported for OffsetDateTime")
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("received more than {limit} bytes without a complete CoT event")]
    FrameTooLarge { limit: usize },
    #[error("invalid CoT XML: {0}")]
    Xml(#[from] quick_xml::Error),
}

/// Metadata extracted from an incoming `CoT` event without interpreting its extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedEvent {
    pub raw: String,
    pub uid: Option<String>,
    pub event_type: Option<String>,
    pub correlation_id: Option<Uuid>,
}

/// # Errors
///
/// Returns an error when `raw` is not a valid `CoT` XML event.
pub fn inspect_event(raw: String) -> Result<ReceivedEvent, DecodeError> {
    let mut reader = Reader::from_reader(raw.as_bytes());
    let mut scratch = Vec::new();
    let mut uid = None;
    let mut event_type = None;
    let mut correlation_id = None;
    loop {
        match reader.read_event_into(&mut scratch)? {
            Event::Start(element) | Event::Empty(element)
                if element.name().as_ref() == b"event" =>
            {
                for attribute in element.attributes().flatten() {
                    match attribute.key.as_ref() {
                        b"uid" => {
                            uid = Some(
                                String::from_utf8_lossy(attribute.value.as_ref()).into_owned(),
                            );
                        }
                        b"type" => {
                            event_type = Some(
                                String::from_utf8_lossy(attribute.value.as_ref()).into_owned(),
                            );
                        }
                        _ => {}
                    }
                }
            }
            Event::Start(element) | Event::Empty(element)
                if element.name().as_ref() == b"tak_bench" =>
            {
                for attribute in element.attributes().flatten() {
                    if attribute.key.as_ref() == b"correlation_id" {
                        correlation_id = String::from_utf8_lossy(attribute.value.as_ref())
                            .parse()
                            .ok();
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        scratch.clear();
    }
    Ok(ReceivedEvent {
        raw,
        uid,
        event_type,
        correlation_id,
    })
}

/// Incrementally collects complete `<event>…</event>` documents from a TCP stream.
#[derive(Debug)]
pub struct CotStreamDecoder {
    buffer: Vec<u8>,
    max_event_bytes: usize,
}

impl CotStreamDecoder {
    #[must_use]
    pub fn new(max_event_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_event_bytes,
        }
    }

    /// # Errors
    ///
    /// Returns an error for an over-sized incomplete frame or malformed XML.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, DecodeError> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        loop {
            let Some(start) = self.buffer.windows(6).position(|w| w == b"<event") else {
                // Preserve enough trailing bytes for a split `<event` marker.
                if self.buffer.len() > 5 {
                    self.buffer.drain(..self.buffer.len() - 5);
                }
                break;
            };
            if start > 0 {
                // TAK Server explicitly ignores preambles, including anonymous `<auth>` payloads.
                self.buffer.drain(..start);
            }
            let Some(end) = self.buffer.windows(8).position(|w| w == b"</event>") else {
                if self.buffer.len() > self.max_event_bytes {
                    return Err(DecodeError::FrameTooLarge {
                        limit: self.max_event_bytes,
                    });
                }
                break;
            };
            let end = end + 8;
            if end > self.max_event_bytes {
                self.buffer.drain(..end);
                return Err(DecodeError::FrameTooLarge {
                    limit: self.max_event_bytes,
                });
            }
            let frame: Vec<u8> = self.buffer.drain(..end).collect();
            validate_xml(&frame)?;
            events.push(String::from_utf8_lossy(&frame).into_owned());
        }
        Ok(events)
    }
}

fn validate_xml(frame: &[u8]) -> Result<(), quick_xml::Error> {
    let mut reader = Reader::from_reader(frame);
    let mut scratch = Vec::new();
    loop {
        match reader.read_event_into(&mut scratch)? {
            quick_xml::events::Event::Eof => return Ok(()),
            _ => scratch.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_accepts_fragmented_and_concatenated_events() {
        let mut decoder = CotStreamDecoder::new(1024);
        assert!(decoder.push(b"<event uid=\"a\">").unwrap().is_empty());
        let result = decoder.push(b"</event><event uid=\"b\"></event>").unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn decoder_discards_anonymous_auth_preamble() {
        let mut decoder = CotStreamDecoder::new(1024);
        let result = decoder
            .push(b"<auth>ignored</auth><event uid=\"a\"></event>")
            .unwrap();
        assert_eq!(result.len(), 1);
    }
}
