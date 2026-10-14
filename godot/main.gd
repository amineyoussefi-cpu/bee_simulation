extends Node2D
## Vue de l'essaim. Godot ne simule rien : il affiche les snapshots Python et envoie des commandes.

const DEFAULT_URL := "ws://127.0.0.1:8765"
const WORLD_ORIGIN := Vector2(16, 16)
const WORLD_SCALE := 0.94
const PANEL_X := 966.0
const SITE_COLORS := {"A": Color(0.90, 0.36, 0.25), "B": Color(0.26, 0.55, 0.92), "C": Color(0.32, 0.74, 0.36)}
const UNDECIDED := Color(0.62, 0.62, 0.62)
const STATE_SHORT := {"REPOS": "R", "EXPLORATION": "E", "INSPECTION": "I", "RETOUR": "T", "DANSE": "D"}

var ws := WebSocketPeer.new()
var ws_url := DEFAULT_URL
var was_open := false
var reconnect_in := 0.0
var snapshot := {}
var metrics := {}
var status := {}
var bee_pos := {}
var selected_bee := -1
var anim_t := 0.0
var ui_refresh := 0.0

var status_label: RichTextLabel
var metrics_label: RichTextLabel
var bee_label: RichTextLabel
var seed_edit: LineEdit
var mode_button: OptionButton
var site_buttons := {}


func _ready() -> void:
	var env_url := OS.get_environment("ESSAIM_WS_URL")
	if env_url != "":
		ws_url = env_url
	# options de vérification : ESSAIM_SELECT_BEE=0 présélectionne une abeille,
	# ESSAIM_SCREENSHOT=/chemin.png enregistre une capture après 6 s puis quitte
	if OS.get_environment("ESSAIM_SELECT_BEE").is_valid_int():
		selected_bee = int(OS.get_environment("ESSAIM_SELECT_BEE"))
	var shot := OS.get_environment("ESSAIM_SCREENSHOT")
	if shot != "":
		get_tree().create_timer(6.0).timeout.connect(func():
			get_viewport().get_texture().get_image().save_png(shot)
			get_tree().quit())
	_build_ui()
	_connect()


func _connect() -> void:
	ws = WebSocketPeer.new()
	ws.inbound_buffer_size = 1 << 20
	var err := ws.connect_to_url(ws_url)
	if err != OK:
		push_warning("Connexion WebSocket impossible (%s) : %s" % [ws_url, error_string(err)])


func _process(delta: float) -> void:
	anim_t += delta
	ws.poll()
	var state := ws.get_ready_state()
	if state == WebSocketPeer.STATE_OPEN:
		was_open = true
		while ws.get_available_packet_count() > 0:
			_on_message(ws.get_packet().get_string_from_utf8())
	elif state == WebSocketPeer.STATE_CLOSED:
		if was_open:
			was_open = false
			status = {}
		reconnect_in -= delta
		if reconnect_in <= 0.0:
			reconnect_in = 2.0
			_connect()
	for bee in snapshot.get("bees", []):
		var id := int(bee["id"])
		var target := _to_screen(Vector2(bee["x"], bee["y"]))
		bee_pos[id] = bee_pos.get(id, target).lerp(target, minf(1.0, delta * 10.0))
	ui_refresh -= delta
	if ui_refresh <= 0.0:
		ui_refresh = 0.2
		_refresh_ui()
	queue_redraw()


func _on_message(text: String) -> void:
	var msg = JSON.parse_string(text)
	if typeof(msg) != TYPE_DICTIONARY:
		return
	match msg.get("type", ""):
		"snapshot":
			if msg["run_id"] != snapshot.get("run_id", ""):
				bee_pos.clear()
				metrics = {}  # les indicateurs d'un ancien run ne s'affichent jamais sur le nouveau
			snapshot = msg
		"metrics":
			if msg["run_id"] == snapshot.get("run_id", ""):
				metrics = msg
		"status":
			if msg["mode"] != status.get("mode", ""):
				mode_button.select(0 if msg["mode"] == "llm" else 1)
			status = msg
		"ack":
			if not msg.get("ok", false):
				push_warning("Commande refusée : %s" % msg)


func _send(cmd: Dictionary) -> void:
	if ws.get_ready_state() == WebSocketPeer.STATE_OPEN:
		ws.send_text(JSON.stringify(cmd))


func _to_screen(p: Vector2) -> Vector2:
	return WORLD_ORIGIN + p * WORLD_SCALE


# ---------------------------------------------------------------- dessin
func _draw() -> void:
	var font := ThemeDB.fallback_font
	var world: Dictionary = snapshot.get("world", {"width": 1000, "height": 600})
	draw_rect(Rect2(WORLD_ORIGIN, Vector2(world["width"], world["height"]) * WORLD_SCALE), Color(0.93, 0.96, 0.88))
	if snapshot.is_empty():
		draw_string(font, WORLD_ORIGIN + Vector2(20, 40), "En attente du serveur Python (%s)…" % ws_url,
				HORIZONTAL_ALIGNMENT_LEFT, -1, 18, Color.DIM_GRAY)
		return
	# branche
	var b: Dictionary = snapshot["branch"]
	var bc := _to_screen(Vector2(b["x"], b["y"]))
	draw_line(bc + Vector2(-120, 6), bc + Vector2(120, -6), Color(0.45, 0.30, 0.15), 10.0)
	draw_circle(bc, b["radius"] * WORLD_SCALE * 1.6, Color(0.45, 0.30, 0.15, 0.12))
	draw_string(font, bc + Vector2(-30, 52), "Branche", HORIZONTAL_ALIGNMENT_LEFT, -1, 14, Color(0.35, 0.22, 0.1))
	# abris
	for s in snapshot["sites"]:
		var c := _to_screen(Vector2(s["x"], s["y"]))
		var col: Color = SITE_COLORS.get(s["id"], Color.GRAY)
		draw_circle(c, 50 * WORLD_SCALE, Color(col, 0.10))
		draw_rect(Rect2(c - Vector2(22, 18), Vector2(44, 36)), col)
		draw_rect(Rect2(c - Vector2(6, 2), Vector2(12, 12)), Color(0.15, 0.1, 0.05))
		draw_string(font, c + Vector2(-26, -26), "%s · %s" % [s["id"], s["name"]], HORIZONTAL_ALIGNMENT_LEFT, -1, 14, col.darkened(0.3))
		if not s["accessible"]:
			draw_line(c - Vector2(26, 22), c + Vector2(26, 22), Color.BLACK, 4.0)
			draw_line(c + Vector2(-26, 22), c + Vector2(26, -22), Color.BLACK, 4.0)
			draw_string(font, c + Vector2(-38, 40), "inaccessible", HORIZONTAL_ALIGNMENT_LEFT, -1, 13, Color.BLACK)
	# abeilles
	var llm_bees: Array = snapshot.get("llm_bees", [])
	for bee in snapshot["bees"]:
		var id := int(bee["id"])
		var p: Vector2 = bee_pos.get(id, _to_screen(Vector2(bee["x"], bee["y"])))
		var pref = bee["preference"]
		var col: Color = SITE_COLORS.get(pref, UNDECIDED) if pref != null else UNDECIDED
		var flap := sin(anim_t * 28.0 + id) * 2.0
		if bee["state"] == "DANSE":
			p += Vector2(cos(anim_t * 9.0 + id), sin(anim_t * 18.0 + id)) * 6.0
		draw_circle(p + Vector2(-5, -6 - flap), 4.0, Color(1, 1, 1, 0.8))
		draw_circle(p + Vector2(5, -6 - flap), 4.0, Color(1, 1, 1, 0.8))
		draw_circle(p, 7.0, col)
		draw_arc(p, 7.0, 0, TAU, 16, Color(0.1, 0.1, 0.1), 1.5)
		if id in llm_bees:
			draw_arc(p, 10.0, 0, TAU, 20, Color(0.55, 0.2, 0.75), 2.0)
		if id == selected_bee:
			draw_arc(p, 14.0, 0, TAU, 24, Color.BLACK, 2.0)
		draw_string(font, p + Vector2(9, 4), "%d%s" % [id, STATE_SHORT.get(bee["state"], "?")],
				HORIZONTAL_ALIGNMENT_LEFT, -1, 11, Color(0.15, 0.15, 0.15))
	draw_string(font, WORLD_ORIGIN + Vector2(8, 600 * WORLD_SCALE - 8),
			"R repos · E exploration · I inspection · T retour · D danse · cercle violet = abeille LLM · gris = indécise",
			HORIZONTAL_ALIGNMENT_LEFT, -1, 12, Color(0.3, 0.3, 0.3))


func _unhandled_input(event: InputEvent) -> void:
	if event is InputEventMouseButton and event.pressed and event.button_index == MOUSE_BUTTON_LEFT:
		var best := -1
		var best_d := 16.0
		for id in bee_pos:
			var d: float = bee_pos[id].distance_to(event.position)
			if d < best_d:
				best_d = d
				best = id
		if best >= 0 or event.position.x < PANEL_X:
			selected_bee = best


# ---------------------------------------------------------------- interface
func _build_ui() -> void:
	var layer := CanvasLayer.new()
	add_child(layer)
	var panel := PanelContainer.new()
	panel.position = Vector2(PANEL_X, 8)
	panel.size = Vector2(1280 - PANEL_X - 8, 704)
	layer.add_child(panel)
	var box := VBoxContainer.new()
	panel.add_child(box)
	status_label = _rich(box, 92)
	metrics_label = _rich(box, 170)
	var row := HBoxContainer.new()
	box.add_child(row)
	_button(row, "Pause", func(): _send({"cmd": "pause"}))
	_button(row, "Reprise", func(): _send({"cmd": "resume"}))
	var row2 := HBoxContainer.new()
	box.add_child(row2)
	seed_edit = LineEdit.new()
	seed_edit.placeholder_text = "graine"
	seed_edit.custom_minimum_size.x = 70
	row2.add_child(seed_edit)
	mode_button = OptionButton.new()
	mode_button.add_item("llm")
	mode_button.add_item("regles")
	row2.add_child(mode_button)
	_button(row2, "Réinitialiser", _on_reset)
	var row3 := HBoxContainer.new()
	box.add_child(row3)
	for site_id in ["A", "B", "C"]:
		site_buttons[site_id] = _button(row3, "Bloquer %s" % site_id, _on_toggle_site.bind(site_id))
	bee_label = _rich(box, 0)
	bee_label.size_flags_vertical = Control.SIZE_EXPAND_FILL
	bee_label.scroll_active = true


func _rich(parent: Control, min_h: float) -> RichTextLabel:
	var label := RichTextLabel.new()
	label.bbcode_enabled = true
	label.custom_minimum_size = Vector2(290, min_h)
	label.add_theme_font_size_override("normal_font_size", 12)
	label.add_theme_font_size_override("bold_font_size", 12)
	label.add_theme_font_size_override("italics_font_size", 12)
	label.fit_content = min_h > 0
	parent.add_child(label)
	return label


func _button(parent: Control, text: String, callback: Callable) -> Button:
	var button := Button.new()
	button.text = text
	button.pressed.connect(callback)
	parent.add_child(button)
	return button


func _on_reset() -> void:
	var cmd := {"cmd": "reset", "mode": mode_button.get_item_text(mode_button.selected)}
	if seed_edit.text.is_valid_int():
		cmd["seed"] = int(seed_edit.text)
	selected_bee = -1
	_send(cmd)


func _on_toggle_site(site_id: String) -> void:
	for s in snapshot.get("sites", []):
		if s["id"] == site_id:
			_send({"cmd": "set_site_accessible", "site": site_id, "accessible": not s["accessible"]})


func _refresh_ui() -> void:
	var open := ws.get_ready_state() == WebSocketPeer.STATE_OPEN
	var t := "[b]Statut[/b]\nPython : %s\n" % ("[color=green]connecté[/color]" if open else "[color=red]déconnecté — reconnexion…[/color]")
	if open and not status.is_empty():
		var kafka_ok: bool = status["kafka_producer"] == "ok" and status["kafka_consumer"] == "ok"
		var since = status.get("kafka_last_consumed_s")
		var stale: bool = since == null or float(since) > 5.0
		t += "Kafka : %s (prod. %s, cons. %s, attente %d, perdus %d%s)\n" % [
			"[color=green]ok[/color]" if kafka_ok and not stale else "[color=red]PANNE[/color]",
			status["kafka_producer"], status["kafka_consumer"], status["kafka_pending"], status["kafka_lost"],
			"" if since == null else ", dernier msg il y a %ss" % since]
		var ollama_ok: bool = status["ollama"] == "prêt"
		t += "Ollama : %s — %s\n" % ["[color=green]prêt[/color]" if ollama_ok else "[color=orange]%s[/color]" % status["ollama"], status["ollama_model"]]
		t += "Mode : %s" % status["mode"]
	status_label.text = t
	if snapshot.is_empty():
		return
	var m := "[b]Run[/b] %s · graine %d · t = %.1f s%s\n" % [snapshot["run_id"], int(snapshot["seed"]), snapshot["sim_time"],
		" · [color=red]PAUSE[/color]" if snapshot["paused"] else (" · [color=red]TERMINÉ[/color]" if snapshot["ended"] else "")]
	if metrics.is_empty():
		m += "[color=orange]Aucun indicateur reçu du consommateur Kafka pour ce run.[/color]"
	else:
		var lag: float = snapshot["sim_time"] - metrics["sim_time"]
		m += "[b]Indicateurs (consommateur Kafka, t = %.1f s%s)[/b]\n" % [metrics["sim_time"], " [color=red]en retard %.0f s[/color]" % lag if lag > 3.0 else ""]
		for site_id in metrics["supports"]:
			m += "[color=#%s]■[/color] %s : %d   " % [SITE_COLORS.get(site_id, Color.GRAY).to_html(false), site_id, metrics["supports"][site_id]]
		m += "\nIndécises : %d · site en tête : %s (%d %%)\n" % [metrics["undecided"], str(metrics["top_site"]), roundi(metrics["top_fraction"] * 100)]
		m += "Consensus (%s) : " % metrics["consensus_rule"]
		if metrics["consensus_time"] == null:
			m += "pas encore\n"
		else:
			m += "%s à t = %.1f s%s\n" % [metrics["consensus_site"], metrics["consensus_time"], " (en cours)" if metrics["consensus_now"] else " (perdu)"]
			m += "Stabilité : %s · ruptures : %d\n" % ["—" if metrics["consensus_stability"] == null else "%d %%" % roundi(metrics["consensus_stability"] * 100), metrics["consensus_breaks"]]
		m += "Communications : %d · événements : %d" % [metrics["communications"], metrics["events_applied"]]
	metrics_label.text = m
	for s in snapshot["sites"]:
		site_buttons[s["id"]].text = ("Bloquer %s" if s["accessible"] else "Ouvrir %s") % s["id"]
	bee_label.text = _bee_text()


func _bee_text() -> String:
	if selected_bee < 0:
		return "[i]Cliquez sur une abeille pour l'inspecter.[/i]"
	var bee: Dictionary = {}
	for b in snapshot["bees"]:
		if int(b["id"]) == selected_bee:
			bee = b
	if bee.is_empty():
		return ""
	var t := "[b]Abeille %d[/b]%s\nProfil : %s · état : %s\n" % [selected_bee,
		" (LLM)" if selected_bee in snapshot.get("llm_bees", []) else "", bee["profile"], bee["state"]]
	t += "Préférence : %s\n" % ("aucune" if bee["preference"] == null else "%s (score %s)" % [bee["preference"], bee["preference_score"]])
	if bee["pending_decision"] != null:
		t += "[color=purple]En attente d'une décision LLM (%s)…[/color]\n" % bee["pending_decision"]
	var d = bee["last_decision"]
	if d != null:
		var src_col: String = {"llm": "purple", "règles": "#dddddd", "fallback": "red"}.get(d["source"], "black")
		t += "Dernière décision : [color=%s]%s[/color] → %s %s (t=%.1f)\n" % [src_col, d["source"].to_upper(), d["action"], "" if d["site"] == null else d["site"], d["t"]]
		if d.has("info"):
			var info: Dictionary = d["info"]
			if info.get("fallback_reason") != null:
				t += "  [color=red]repli : %s[/color]\n" % info["fallback_reason"]
			elif info.has("latency_ms"):
				t += "  latence %s ms, tokens dispo %s\n" % [info["latency_ms"], info.get("tokens_available")]
	t += "[b]Observations personnelles[/b]\n"
	for o in bee["observations"]:
		t += "  • %s t=%.0f : %s\n" % [o["site"], o["t"], ("score %s" % o["score"]) if o["accessible"] else "INACCESSIBLE"]
	t += "[b]Informations reçues[/b]\n"
	for r in bee["received"]:
		t += "  • de %d t=%.0f : %s %s\n" % [r["from"], r["t"], r["site"], "recommandé" if r["accessible"] else "inaccessible"]
	t += "[i]Mémoire : %d / 8 souvenirs[/i]" % bee["memory"].size()
	return t
