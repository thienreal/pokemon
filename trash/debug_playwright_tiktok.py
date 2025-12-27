from playwright.sync_api import sync_playwright

def main():
    url = 'https://www.tiktok.com/tag/danang?lang=en'
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(locale='en-US')
        page = ctx.new_page()
        page.goto(url, wait_until='domcontentloaded', timeout=60000)
        html = page.content()
        print('title:', page.title())
        print('SIGI_STATE in html:', 'SIGI_STATE' in html)
        print('__NEXT_DATA__ in html:', '__NEXT_DATA__' in html)
        # Dump a small slice for inspection
        start = html.find('SIGI_STATE')
        print('index SIGI_STATE:', start)
        if start != -1:
            print(html[start:start+200])
        browser.close()

if __name__ == '__main__':
    main()
