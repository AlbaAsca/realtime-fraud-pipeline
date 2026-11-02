"""Graba docs/img/demo.gif y docs/img/panel.png con el panel en marcha, y mide la latencia de las alertas.

    GENERATOR_ATTACK_EVERY=6s docker compose up --build --detach
    pip install playwright pillow
    python docs/capture_demo.py <ruta-de-chrome> [segundos-extra-midiendo-latencia]
"""

import io
import statistics
import sys
import time
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

URL = "http://localhost:8080"
OUT = Path(__file__).parent / "img"
FRAMES, EVERY_S, PLAY_MS, WIDTH = 48, 1.0, 350, 860
LATENCY_SAMPLING_S = int(sys.argv[2]) if len(sys.argv) > 2 else 0

# Un EventSource propio: compara el instante del evento con el de llegada al navegador.
MEASURE_LATENCY = """
window.latencies = [];
new EventSource('/api/alerts/events').addEventListener('alert', event => {
    window.latencies.push(Date.now() - new Date(JSON.parse(event.data).occurredAt).getTime());
});
"""

with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=sys.argv[1])
    page = browser.new_page(viewport={"width": 1120, "height": 800})
    page.goto(URL)
    page.evaluate(MEASURE_LATENCY)
    page.wait_for_selector("#alerts .item")
    time.sleep(3)

    shots = []
    for i in range(FRAMES):
        if i % 16 == 8:
            page.locator("#alerts .item").first.click()
        shots.append(Image.open(io.BytesIO(page.screenshot())).convert("RGB"))
        time.sleep(EVERY_S)

    shots[-1].save(OUT / "panel.png", optimize=True)
    time.sleep(LATENCY_SAMPLING_S)
    samples = page.evaluate("window.latencies")
    browser.close()

height = round(shots[0].height * WIDTH / shots[0].width)
small = [shot.resize((WIDTH, height), Image.LANCZOS) for shot in shots]
# Una sola paleta para todos los fotogramas: sin parpadeo de color y mucho menos peso.
palette = Image.new("RGB", (WIDTH, height * 4))
for i, frame in enumerate(small[:: max(1, len(small) // 4)][:4]):
    palette.paste(frame, (0, i * height))
palette = palette.quantize(colors=256, method=Image.FASTOCTREE)
frames = [frame.quantize(palette=palette, dither=Image.NONE) for frame in small]
frames[0].save(OUT / "demo.gif", save_all=True, append_images=frames[1:], duration=PLAY_MS, loop=0, optimize=True)

print(f"demo.gif: {(OUT / 'demo.gif').stat().st_size / 1e6:.1f} MB")
if samples:
    print(f"latencia de alertas: n={len(samples)}, mediana={statistics.median(samples):.0f} ms, máx={max(samples)} ms")
