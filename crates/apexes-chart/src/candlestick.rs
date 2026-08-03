//! `CandlestickChart`: a hand-written-`wgpu`-rendered K-line chart
//! widget, embedded into an `egui` frame via `egui_wgpu::CallbackTrait`.
//! Owns its own `wgpu::RenderPipeline` (built once at construction
//! time, per `pipeline.rs`), and on every `show` call builds a fresh
//! vertex buffer from the current `bars`/`ViewState`/Y-range via
//! `geometry::candlestick_vertices`, then issues an
//! `egui_wgpu::Callback::new_paint_callback` to draw it. Also reads
//! scroll/drag input from the allocated response rect each frame and
//! feeds it into the shared `ViewState`'s `zoom`/`pan`, mirroring
//! `egui_plot`'s interaction conventions (see the design spec §4.4).

use crate::geometry::{Vertex, candlestick_vertices};
use crate::pipeline::create_solid_color_pipeline;
use crate::view_state::ViewState;
use apexes_core::Bar;
use rust_decimal::Decimal;
use std::sync::Arc;
use wgpu::util::DeviceExt;

const SHADER_SOURCE: &str = include_str!("shaders/candlestick.wgsl");

pub struct CandlestickChart {
    pipeline: Arc<wgpu::RenderPipeline>,
}

impl CandlestickChart {
    pub fn new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self {
        Self {
            pipeline: Arc::new(create_solid_color_pipeline(
                device,
                target_format,
                SHADER_SOURCE,
            )),
        }
    }

    /// Renders the chart for `bars` (any mix of closed bars and the
    /// current still-forming bar) into a `desired_size`-sized area of
    /// `ui`, reading scroll/drag input from that area to update
    /// `view` (shared with `TickLineChart` by the caller).
    pub fn show(
        &self,
        ui: &mut egui::Ui,
        bars: &[Bar],
        view: &mut ViewState,
        desired_size: egui::Vec2,
    ) {
        let (rect, response) = ui.allocate_exact_size(desired_size, egui::Sense::click_and_drag());

        if response.hovered() {
            let scroll_delta = ui.input(|i| i.smooth_scroll_delta.y);
            if scroll_delta != 0.0
                && let Some(hover_pos) = response.hover_pos()
            {
                let cursor_fraction = ((hover_pos.x - rect.left()) / rect.width()).clamp(0.0, 1.0);
                view.zoom(cursor_fraction, scroll_delta * 0.01);
            }
        }
        if response.dragged() {
            let drag_delta_fraction = -response.drag_delta().x / rect.width();
            view.pan(drag_delta_fraction);
        }

        let y_range = view
            .y_axis_range_for(bars.iter().flat_map(|b| [b.high, b.low].into_iter()))
            .unwrap_or(Decimal::ZERO..Decimal::ONE);
        let vertices = candlestick_vertices(bars, view, &y_range);

        let pipeline = Arc::clone(&self.pipeline);
        let callback = egui_wgpu::Callback::new_paint_callback(
            rect,
            CandlestickPaintCallback { pipeline, vertices },
        );
        ui.painter().add(callback);
    }
}

struct CandlestickPaintCallback {
    pipeline: Arc<wgpu::RenderPipeline>,
    vertices: Vec<Vertex>,
}

impl egui_wgpu::CallbackTrait for CandlestickPaintCallback {
    fn prepare(
        &self,
        device: &wgpu::Device,
        _queue: &wgpu::Queue,
        _screen_descriptor: &egui_wgpu::ScreenDescriptor,
        _egui_encoder: &mut wgpu::CommandEncoder,
        callback_resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        let buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("apexes-chart candlestick vertex buffer"),
            contents: bytemuck::cast_slice(&self.vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        callback_resources.insert(CandlestickFrameBuffer {
            buffer,
            vertex_count: self.vertices.len() as u32,
        });
        Vec::new()
    }

    fn paint(
        &self,
        _info: egui::PaintCallbackInfo,
        render_pass: &mut wgpu::RenderPass<'static>,
        callback_resources: &egui_wgpu::CallbackResources,
    ) {
        let Some(frame_buffer) = callback_resources.get::<CandlestickFrameBuffer>() else {
            return;
        };
        if frame_buffer.vertex_count == 0 {
            return;
        }
        render_pass.set_pipeline(&self.pipeline);
        render_pass.set_vertex_buffer(0, frame_buffer.buffer.slice(..));
        render_pass.draw(0..frame_buffer.vertex_count, 0..1);
    }
}

struct CandlestickFrameBuffer {
    buffer: wgpu::Buffer,
    vertex_count: u32,
}
