extends SceneTree
## Test headless : se connecte au serveur Python, attend un snapshot, envoie pause/reprise, vérifie l'accusé.
## godot --headless --path godot --script res://tests/ws_smoke.gd

var ws := WebSocketPeer.new()
var elapsed := 0.0
var got_snapshot := false
var acks := 0


func _initialize() -> void:
	var url := OS.get_environment("ESSAIM_WS_URL")
	ws.inbound_buffer_size = 1 << 20
	ws.connect_to_url(url if url != "" else "ws://127.0.0.1:8765")


func _process(delta: float) -> bool:
	elapsed += delta
	ws.poll()
	if ws.get_ready_state() == WebSocketPeer.STATE_OPEN:
		while ws.get_available_packet_count() > 0:
			var msg = JSON.parse_string(ws.get_packet().get_string_from_utf8())
			if msg["type"] == "snapshot" and not got_snapshot:
				got_snapshot = true
				print("snapshot reçu : run %s, %d abeilles, %d abris" % [msg["run_id"], msg["bees"].size(), msg["sites"].size()])
				ws.send_text(JSON.stringify({"cmd": "pause"}))
				ws.send_text(JSON.stringify({"cmd": "resume"}))
			elif msg["type"] == "ack" and msg["ok"]:
				acks += 1
	if got_snapshot and acks >= 2:
		print("GODOT_WS_OK")
		quit(0)
	elif elapsed > 15.0:
		print("GODOT_WS_ECHEC (snapshot=%s, acks=%d)" % [got_snapshot, acks])
		quit(1)
	return false
