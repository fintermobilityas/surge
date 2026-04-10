use eframe::egui::{self, Align2, Color32, CornerRadius, FontId, Pos2, Shape, Stroke, StrokeKind, Vec2};

use super::theme::{
    ACCENT, ACCENT_HOVER, ACCENT_PRESSED, ERROR, PROGRESS_TRACK, SUCCESS, TEXT_MUTED, TEXT_PRIMARY, TEXT_SECONDARY,
};

pub(super) fn accent_button(ui: &mut egui::Ui, text: &str, width: f32, height: f32) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::click());
    let hovered = response.hovered();
    let pressed = response.is_pointer_button_down_on();

    if ui.is_rect_visible(rect) {
        let painter = ui.painter();
        let rounding = CornerRadius::same(10);

        let bg = if pressed {
            ACCENT_PRESSED
        } else if hovered {
            ACCENT_HOVER
        } else {
            ACCENT
        };

        painter.rect_filled(rect, rounding, bg);

        if hovered && !pressed {
            painter.rect_stroke(
                rect.expand(1.5),
                CornerRadius::same(12),
                Stroke::new(1.0, ACCENT.linear_multiply(0.35)),
                StrokeKind::Outside,
            );
        }

        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            text,
            FontId::proportional(15.0),
            Color32::WHITE,
        );
    }

    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

pub(super) fn ghost_button(ui: &mut egui::Ui, text: &str, width: f32, height: f32) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::click());
    let hovered = response.hovered();

    if ui.is_rect_visible(rect) {
        let painter = ui.painter();
        let rounding = CornerRadius::same(10);

        if hovered {
            painter.rect_filled(rect, rounding, Color32::from_rgba_premultiplied(255, 255, 255, 6));
        }

        let border = if hovered { TEXT_SECONDARY } else { TEXT_MUTED };
        let text_c = if hovered { TEXT_PRIMARY } else { TEXT_SECONDARY };

        painter.rect_stroke(rect, rounding, Stroke::new(1.0, border), StrokeKind::Outside);
        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            text,
            FontId::proportional(14.0),
            text_c,
        );
    }

    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

pub(super) fn draw_progress_bar(ui: &mut egui::Ui, progress: f32, width: f32, height: f32) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let rounding = CornerRadius::same((height / 2.0) as u8);

    painter.rect_filled(rect, rounding, PROGRESS_TRACK);

    let fill_w = rect.width() * progress.clamp(0.0, 1.0);
    if fill_w > 1.0 {
        let fill = egui::Rect::from_min_size(rect.min, Vec2::new(fill_w, height));
        painter.rect_filled(fill, rounding, ACCENT);

        let hi = egui::Rect::from_min_size(rect.min, Vec2::new(fill_w, height * 0.45));
        painter.rect_filled(hi, rounding, ACCENT_HOVER.linear_multiply(0.18));
    }
}

pub(super) fn draw_checkmark(ui: &mut egui::Ui, radius: f32) {
    let size = Vec2::splat(radius * 2.0 + 8.0);
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let center = rect.center();

    painter.circle_filled(center, radius, SUCCESS.linear_multiply(0.12));
    painter.circle_stroke(center, radius, Stroke::new(2.0, SUCCESS));

    let half = radius * 0.45;
    let p1 = Pos2::new(center.x - half, center.y);
    let p2 = Pos2::new(center.x - half * 0.3, center.y + half * 0.7);
    let p3 = Pos2::new(center.x + half, center.y - half * 0.5);
    painter.line_segment([p1, p2], Stroke::new(2.5, SUCCESS));
    painter.line_segment([p2, p3], Stroke::new(2.5, SUCCESS));
}

pub(super) fn draw_bolt(ui: &mut egui::Ui, height: f32, fill: Color32, outline: Color32) {
    let width = height * 0.5;
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }
    let painter = ui.painter();
    let center = rect.center();
    let scale = height / 180.0;
    let pt = |x: f32, y: f32| Pos2::new(center.x + (x - 100.0) * scale, center.y + (y - 100.0) * scale);
    let verts = [
        pt(110.0, 10.0),
        pt(60.0, 95.0),
        pt(95.0, 95.0),
        pt(80.0, 190.0),
        pt(140.0, 105.0),
        pt(105.0, 105.0),
    ];

    painter.circle_filled(center, height * 0.38, fill.linear_multiply(0.08));
    painter.circle_filled(center, height * 0.28, fill.linear_multiply(0.06));

    let no_stroke = Stroke::NONE;
    painter.add(Shape::convex_polygon(
        vec![verts[0], verts[1], verts[5]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[1], verts[2], verts[5]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[2], verts[3], verts[4]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[2], verts[4], verts[5]],
        fill,
        no_stroke,
    ));

    painter.add(Shape::closed_line(verts.to_vec(), Stroke::new(1.5, outline)));
}

pub(super) fn draw_x_mark(ui: &mut egui::Ui, radius: f32) {
    let size = Vec2::splat(radius * 2.0 + 8.0);
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let center = rect.center();

    painter.circle_filled(center, radius, ERROR.linear_multiply(0.12));
    painter.circle_stroke(center, radius, Stroke::new(2.0, ERROR));

    let spread = radius * 0.38;
    painter.line_segment(
        [
            Pos2::new(center.x - spread, center.y - spread),
            Pos2::new(center.x + spread, center.y + spread),
        ],
        Stroke::new(2.5, ERROR),
    );
    painter.line_segment(
        [
            Pos2::new(center.x + spread, center.y - spread),
            Pos2::new(center.x - spread, center.y + spread),
        ],
        Stroke::new(2.5, ERROR),
    );
}
