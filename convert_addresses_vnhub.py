import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
from tqdm import tqdm


# Heuristic selectors and strategies to interact with the site.
INPUT_CANDIDATES: List[str] = [
    'input[placeholder*="địa chỉ cũ" i]',
    'textarea[placeholder*="địa chỉ cũ" i]',
    '#oldAddress',
    'input[name="old_address"]',
    'textarea[name="old_address"]',
]

BUTTON_CANDIDATES: List[str] = [
    'button:has-text("Chuyển đổi")',
    'text=Chuyển đổi',
    'button:has-text("Convert")',
    'button[type="submit"]',
]

OUTPUT_CANDIDATES: List[str] = [
    'input[placeholder*="địa chỉ mới" i]',
    'textarea[placeholder*="địa chỉ mới" i]',
    '#newAddress',
    'input[name="new_address"]',
    'textarea[name="new_address"]',
]

NETWORK_KEYS: List[str] = [
    'newAddress', 'addr_new', 'addressNew', 'diaChiMoi', 'new_address'
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert old addresses to new ones via vnhub tool.")
    p.add_argument("--input", default="tourism_destinations_province_only_fixed.csv", help="Input CSV path with columns name,address")
    p.add_argument("--output", default="tourism_destinations_province_only_converted.csv", help="Output CSV path")
    p.add_argument("--url", default="https://tienich.vnhub.com/", help="URL of the vnhub tool page")
    p.add_argument("--column", default="address", help="CSV column name to convert")
    p.add_argument("--headless", action="store_true", help="Run browser headless (default)")
    p.add_argument("--headed", action="store_true", help="Run browser headed (override headless)")
    p.add_argument("--limit", type=int, default=0, help="Limit number of rows to process (0 = all)")
    p.add_argument("--start", type=int, default=0, help="Start index (0-based) for resuming")
    p.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between actions")
    p.add_argument("--timeout", type=float, default=15.0, help="Per-item timeout in seconds")
    p.add_argument("--retries", type=int, default=2, help="Retries per item on failure")
    return p.parse_args()


async def try_get_first(page, selectors: List[str]):
    for sel in selectors:
        loc = page.locator(sel)
        try:
            if await loc.count() > 0:
                return loc.first
        except Exception:
            continue
    return None


async def extract_output_text(page) -> Optional[str]:
    # Direct selectors first
    out_loc = await try_get_first(page, OUTPUT_CANDIDATES)
    if out_loc is not None:
        try:
            # Try value() for inputs/textarea
            val = await out_loc.input_value()
            if val:
                return val.strip()
        except Exception:
            # Fallback to text content
            try:
                txt = await out_loc.text_content()
                if txt:
                    return txt.strip()
            except Exception:
                pass

    # Heuristic: find a label containing "Địa chỉ mới" and get next input/textarea
    try:
        label = page.locator("text=/\bĐịa\s+chỉ\s+mới\b/i").first
        if await label.count() > 0:
            container = label.locator("xpath=..")
            candidate = container.locator("xpath=.//input | .//textarea").first
            if await candidate.count() > 0:
                try:
                    val = await candidate.input_value()
                    if val:
                        return val.strip()
                except Exception:
                    txt = await candidate.text_content()
                    if txt:
                        return txt.strip()
    except Exception:
        pass

    return None


async def capture_network_new_addr(page) -> Optional[str]:
    # This function inspects recent XHR/fetch JSON responses for likely fields
    new_val: Optional[str] = None

    def on_response(response):
        nonlocal new_val
        if new_val is not None:
            return
        try:
            ct = response.headers.get("content-type", "")
            if "application/json" in ct:
                # We must await body() in async, but Playwright Python exposes async method json()
                pass
        except Exception:
            return

    page.on("response", on_response)
    # We cannot easily read the bodies synchronously here without awaiting. We'll perform a quick sweep via JS.
    # As a fallback, try to query window.__lastResult if the page sets it (some tools do).
    try:
        data = await page.evaluate("() => window.__lastResult || null")
        if isinstance(data, dict):
            for k in NETWORK_KEYS:
                if k in data and isinstance(data[k], str) and data[k].strip():
                    return data[k].strip()
    except Exception:
        pass
    return new_val


async def convert_one(page, url: str, old_addr: str, delay: float, timeout: float) -> Optional[str]:
    await page.goto(url, wait_until="domcontentloaded")

    # Find input and button
    in_loc = await try_get_first(page, INPUT_CANDIDATES)
    if in_loc is None:
        # Try the first visible input as a last resort
        try:
            vis_inputs = page.locator("input, textarea").filter(has_text="")
            if await vis_inputs.count() > 0:
                in_loc = vis_inputs.first
        except Exception:
            pass
    if in_loc is None:
        raise RuntimeError("Could not locate the address input on the page.")

    await in_loc.click()
    try:
        # Clear if possible
        await in_loc.fill("")
    except Exception:
        pass
    await in_loc.type(old_addr, delay=delay / max(1.0, len(old_addr)))

    btn = await try_get_first(page, BUTTON_CANDIDATES)
    if btn is None:
        # Try pressing Enter in input as fallback
        await in_loc.press("Enter")
    else:
        await btn.click()

    # Wait a short while for the result to populate
    try:
        await page.wait_for_timeout(int(delay * 1000) + 300)
        # First, try output field
        result = await extract_output_text(page)
        if result and result.strip():
            return result.strip()
        # Fallback to network sniffing
        result = await capture_network_new_addr(page)
        if result and result.strip():
            return result.strip()
    except Exception:
        pass

    # As a brute-force fallback, get any text node that looks like an address in a result area
    try:
        possible = await page.locator("text=/\bĐịa\s+chỉ\s+mới\b/i").all_text_contents()
        if possible:
            # The new address might appear alongside the label
            tail = possible[-1].split(":")[-1].strip()
            if tail:
                return tail
    except Exception:
        pass

    return None


async def main_async(args: argparse.Namespace) -> int:
    from playwright.async_api import async_playwright

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"Input file not found: {in_path}", file=sys.stderr)
        return 2

    df = pd.read_csv(in_path)
    if args.column not in df.columns:
        print(f"Column '{args.column}' not found in CSV. Available: {list(df.columns)}", file=sys.stderr)
        return 2

    if 'new_address' not in df.columns:
        df['new_address'] = ''

    start = max(0, args.start)
    end = len(df) if args.limit in (None, 0) else min(len(df), start + args.limit)

    headless = not args.headed

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            rng = range(start, end)
            for i in tqdm(rng, desc="Converting", unit="row"):
                old_addr = str(df.loc[i, args.column]) if pd.notna(df.loc[i, args.column]) else ''
                if not old_addr.strip():
                    df.loc[i, 'new_address'] = ''
                    continue

                success = None
                for attempt in range(args.retries + 1):
                    try:
                        result = await asyncio.wait_for(
                            convert_one(page, args.url, old_addr.strip(), args.delay, args.timeout),
                            timeout=args.timeout
                        )
                        if result is None:
                            raise RuntimeError("No result returned")
                        success = result
                        break
                    except Exception as e:
                        await page.wait_for_timeout(400)
                        if attempt == args.retries:
                            sys.stderr.write(f"[row {i}] Failed to convert '{old_addr}': {e}\n")

                df.loc[i, 'new_address'] = success or ''

                # Periodic checkpoint saves
                if (i - start) % 50 == 0:
                    out_tmp = Path(args.output).with_suffix('.partial.csv')
                    df.iloc[:i+1].to_csv(out_tmp, index=False)

        finally:
            await context.close()
            await browser.close()

    out_path = Path(args.output)
    df.to_csv(out_path, index=False)
    print(f"Saved converted CSV to {out_path}")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
