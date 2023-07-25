use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

const STYLE_TEMPLATE: &str = "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}";
const STYLE_PROGRESS_CHARS: &str = "##-";
const READ_BAR_FINISH_MESSAGE: &str = "Reading finished";
const WRITE_BAR_FINISH_MESSAGE: &str = "Writing finished";

#[derive(Clone)]
pub enum ProgressState {
    Empty,
    Diplayed {
        style: ProgressStyle,
        multi_bar: MultiProgress,
        read_bar: Option<ProgressBar>,
        write_bar: Option<ProgressBar>,
    },
}

impl Default for ProgressState {
    fn default() -> Self {
        Self::Empty
    }
}

impl ProgressState {
    pub fn new() -> Self {
        let style = ProgressStyle::with_template(STYLE_TEMPLATE)
            .unwrap()
            .progress_chars(STYLE_PROGRESS_CHARS);

        Self::Diplayed {
            style,
            multi_bar: MultiProgress::new(),
            read_bar: None,
            write_bar: None,
        }
    }

    pub fn init_read_bar<L: FnOnce() -> usize>(&mut self, len: L) -> Option<ProgressBar> {
        if let Self::Diplayed {
            style,
            multi_bar,
            read_bar,
            ..
        } = self
        {
            let bar = ProgressBar::new(len() as u64);
            bar.set_style(style.clone());
            multi_bar.add(bar.clone());
            *read_bar = Some(bar.clone());
            Some(bar)
        } else {
            None
        }
    }

    pub fn init_write_bar<L: FnOnce() -> usize>(&mut self, len: L) -> Option<ProgressBar> {
        if let Self::Diplayed {
            style,
            multi_bar,
            write_bar,
            ..
        } = self
        {
            let bar = ProgressBar::new(len() as u64);
            bar.set_style(style.clone());
            multi_bar.add(bar.clone());
            *write_bar = Some(bar.clone());
            Some(bar)
        } else {
            None
        }
    }

    pub fn finish_read_bar(&self) {
        if let Self::Diplayed {
            read_bar: Some(read_bar),
            ..
        } = self
        {
            read_bar.finish_with_message(READ_BAR_FINISH_MESSAGE);
        }
    }

    pub fn finish_write_bar(&self) {
        if let Self::Diplayed {
            write_bar: Some(write_bar),
            ..
        } = self
        {
            write_bar.finish_with_message(WRITE_BAR_FINISH_MESSAGE);
        }
    }

    pub fn read_bar(&self) -> Option<ProgressBar> {
        match self {
            Self::Empty => None,
            Self::Diplayed { read_bar, .. } => read_bar.clone(),
        }
    }
}
