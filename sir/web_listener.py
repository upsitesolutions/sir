import http.server
import socketserver
import urllib.parse
import threading
import logging
import uuid
from sqlalchemy import select
from sir.util import db_session
from sir.schema import SCHEMA
from sir.indexing import live_index

logger = logging.getLogger("sir.web_listener")

PORT = 7151

class ReindexHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        query = urllib.parse.parse_qs(parsed_path.query)

        if path == "/reindex":
            gid_list = query.get("gid", [])
            if not gid_list:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"status": "error", "message": "Missing gid parameter"}')
                return

            gid_str = gid_list[0]
            try:
                recording_gid = uuid.UUID(gid_str)
            except ValueError:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"status": "error", "message": "Invalid UUID"}')
                return

            logger.info(f"Received reindex request for GID: {recording_gid}")
            
            try:
                self.trigger_reindex(recording_gid)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "success"}')
            except Exception as e:
                logger.error(f"Reindex failed for {recording_gid}: {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f'{{"status": "error", "message": "{str(e)}"}}'.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def trigger_reindex(self, recording_gid):
        # Resolve GID to ID
        # Logic adapted from reindex_recording.py
        try:
            recording_model = SCHEMA["recording"].model
        except KeyError:
            raise Exception("Recording schema not found")

        session_factory = db_session()
        with session_factory() as session:
            stmt = select(recording_model.id).where(recording_model.gid == recording_gid)
            result = session.execute(stmt).scalar_one_or_none()
            
            if result is None:
                raise Exception(f"Recording with GID {recording_gid} not found")
            
            recording_id = result
            logger.info(f"Found recording ID: {recording_id} for GID: {recording_gid}")
            
            # Trigger live index
            live_index({"recording": {recording_id}})

def run_server():
    # Use TCPServer directly
    with socketserver.TCPServer(("", PORT), ReindexHandler) as httpd:
        logger.info(f"Web listener serving at port {PORT}")
        httpd.serve_forever()

def start_web_listener():
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

if __name__ == "__main__":
    # Ensure logging is configured when running standalone
    logging.basicConfig(level=logging.INFO)
    run_server()
