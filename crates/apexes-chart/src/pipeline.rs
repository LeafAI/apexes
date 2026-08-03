//! Shared `wgpu::RenderPipeline` construction for both
//! `CandlestickChart` and `TickLineChart` -- both charts submit
//! pre-positioned, pre-colored vertices with no transform (see
//! `shaders/candlestick.wgsl`'s own doc comment), so one pipeline
//! shape serves both.

use crate::geometry::Vertex;

/// Creates a `wgpu::RenderPipeline` for `shader_source` (WGSL text),
/// rendering `Vertex` (`[f32; 2]` position + `[f32; 4]` color)
/// triangle lists with no depth/stencil testing and standard alpha
/// blending, targeting `target_format`. A `wgpu` pipeline-creation
/// failure (e.g. malformed WGSL) panics with a clear message rather
/// than silently producing a chart that never renders -- this
/// happens once at `CandlestickChart::new`/`TickLineChart::new` time
/// (called from `apexes-ui`'s `main.rs` at startup), matching this
/// project's established fail-fast-at-startup convention.
pub fn create_solid_color_pipeline(
    device: &wgpu::Device,
    target_format: wgpu::TextureFormat,
    shader_source: &str,
) -> wgpu::RenderPipeline {
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("apexes-chart solid color shader"),
        source: wgpu::ShaderSource::Wgsl(shader_source.into()),
    });

    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
        label: Some("apexes-chart solid color pipeline layout"),
        bind_group_layouts: &[],
        immediate_size: 0,
    });

    let vertex_attributes = wgpu::vertex_attr_array![0 => Float32x2, 1 => Float32x4];

    device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: Some("apexes-chart solid color pipeline"),
        layout: Some(&pipeline_layout),
        vertex: wgpu::VertexState {
            module: &shader,
            entry_point: Some("vs_main"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            buffers: &[wgpu::VertexBufferLayout {
                array_stride: std::mem::size_of::<Vertex>() as wgpu::BufferAddress,
                step_mode: wgpu::VertexStepMode::Vertex,
                attributes: &vertex_attributes,
            }],
        },
        fragment: Some(wgpu::FragmentState {
            module: &shader,
            entry_point: Some("fs_main"),
            compilation_options: wgpu::PipelineCompilationOptions::default(),
            targets: &[Some(wgpu::ColorTargetState {
                format: target_format,
                blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                write_mask: wgpu::ColorWrites::ALL,
            })],
        }),
        primitive: wgpu::PrimitiveState::default(),
        depth_stencil: None,
        multisample: wgpu::MultisampleState::default(),
        multiview_mask: None,
        cache: None,
    })
}
