//! `TickLineChart`: a hand-written-`wgpu`-rendered bid/ask line chart
//! widget, embedded into an `egui` frame via `egui_wgpu::CallbackTrait`.
//! Structurally identical to `CandlestickChart` (see that module's
//! own doc comment for the shared rationale) -- reuses the same
//! `pipeline.rs`/`shaders/candlestick.wgsl` (a generic solid-color
//! shader, not actually candlestick-specific) since both widgets
//! submit pre-positioned, pre-colored triangle-list vertices with no
//! transform.

use crate::geometry::{TickPoint, Vertex, tick_line_vertices};
use crate::pipeline::create_solid_color_pipeline;
use crate::view_state::ViewState;
use rust_decimal::Decimal;
use std::sync::Arc;
use wgpu::util::DeviceExt;

const SHADER_SOURCE: &str = include_str!("shaders/candlestick.wgsl");

pub struct TickLineChart {
    pipeline: Arc<wgpu::RenderPipeline>,
}

impl TickLineChart {
    pub fn new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self {
        Self {
            pipeline: Arc::new(create_solid_color_pipeline(
                device,
                target_format,
                SHADER_SOURCE,
            )),
        }
    }

    /// Renders the chart for `ticks` into a `desired_size`-sized area
    /// of `ui`, reading scroll/drag input from that area to update
    /// `view` (shared with `CandlestickChart` by the caller).
    pub fn show(
        &self,
        ui: &mut egui::Ui,
        ticks: &[TickPoint],
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
            .y_axis_range_for(ticks.iter().flat_map(|t| [t.bid, t.ask].into_iter()))
            .unwrap_or(Decimal::ZERO..Decimal::ONE);
        let vertices = tick_line_vertices(ticks, view, &y_range);

        let pipeline = Arc::clone(&self.pipeline);
        let callback = egui_wgpu::Callback::new_paint_callback(
            rect,
            TickLinePaintCallback { pipeline, vertices },
        );
        ui.painter().add(callback);
    }
}

struct TickLinePaintCallback {
    pipeline: Arc<wgpu::RenderPipeline>,
    vertices: Vec<Vertex>,
}

impl egui_wgpu::CallbackTrait for TickLinePaintCallback {
    fn prepare(
        &self,
        device: &wgpu::Device,
        _queue: &wgpu::Queue,
        _screen_descriptor: &egui_wgpu::ScreenDescriptor,
        _egui_encoder: &mut wgpu::CommandEncoder,
        callback_resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        let buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("apexes-chart tick-line vertex buffer"),
            contents: bytemuck::cast_slice(&self.vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        callback_resources.insert(TickLineFrameBuffer {
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
        let Some(frame_buffer) = callback_resources.get::<TickLineFrameBuffer>() else {
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

struct TickLineFrameBuffer {
    buffer: wgpu::Buffer,
    vertex_count: u32,
}
