import asyncio
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
import httpx
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse


async def parse_website(url: str) -> Optional[Dict[str, Any]]:
    print(f"Парсинг веб-сайту: {url}")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
            }
            response = await client.get(url, follow_redirects=True, headers=headers)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        domain = urlparse(url).netloc

        # --- 1. Заголовок ---
        title = None
        if not title:
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                title = og_title['content'].strip()
        if not title:
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text().strip()
        if not title:
            title = "Заголовок не знайдено"

        # --- 2. Контент ---
        content = ""
        article_body = None

        if "eurointegration.com.ua" in domain:
            article_body = soup.find('div', class_='post_text')
        elif "minprom.ua" in domain:
            article_body = soup.find('div', class_='full-article') or soup.find('div', class_='field__item')
        elif "korrespondent.net" in domain:
            article_body = soup.find('div', class_='post-item__text') or soup.find('div', class_='post-item')
        elif "finance.ua" in domain:
            article_body = soup.find('div', class_='article__text') or soup.find('div', class_='news-text')
        elif "financy.24tv.ua" in domain:
            article_body = soup.find('div', class_='news-text') or soup.find('div', class_='article__body')
        elif "delo.ua" in domain:
            article_body = soup.find('div', class_='article__body') or soup.find('article')

        if article_body:
            paragraphs = article_body.find_all('p')
            content = "\n".join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
        else:
            # fallback
            paragraphs = soup.find_all('p')
            content = "\n".join([p.get_text().strip() for p in paragraphs[:10] if p.get_text().strip()])

        if not content:
            content = "Зміст не знайдено"

        # --- 3. Зображення ---
        image_url = None
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            image_url = og_image['content']
        else:
            img_tag = soup.find('img', src=True)
            if img_tag:
                image_url = img_tag['src']
        if image_url and not image_url.startswith(('http://', 'https://')):
            image_url = urljoin(url, image_url)

        # --- 4. Дата публікації ---
        published_at = datetime.now(timezone.utc)
        pub_date_meta = (
            soup.find('meta', property='article:published_time') or
            soup.find('meta', property='og:updated_time') or
            soup.find('meta', {'name': 'date'}) or
            soup.find('time', datetime=True)
        )
        if pub_date_meta:
            date_str = pub_date_meta.get('content') or pub_date_meta.get('datetime')
            if date_str:
                try:
                    published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        return {
            "title": title,
            "content": content,
            "source_url": url,
            "image_url": image_url,
            "published_at": published_at,
            "lang": "uk"
        }

    except httpx.RequestError as e:
        print(f"HTTP помилка для {url}: {e}")
        return None
    except Exception as e:
        print(f"Помилка при парсингу {url}: {e}")
        return None


# Локальне тестування
if __name__ == "__main__":
    test_urls = [
        "https://www.eurointegration.com.ua/news/2024/10/12/7172815/",
        "https://minprom.ua/news/ukrayinska-promyslovist-zrostaye",
        "https://ua.korrespondent.net/business/economics/4651154",
        "https://news.finance.ua/ua/news/-/531553",
        "https://financy.24tv.ua/novyny-pro-finansy_n2241",
        "https://delo.ua/finance/obligaciyi-v-ukrayini-rostut-v-cini-4454/"
    ]

    async def test():
        for url in test_urls:
            data = await parse_website(url)
            print(f"\n--- {url} ---")
            print(f"Заголовок: {data.get('title')}")
            print(f"Дата: {data.get('published_at')}")
            print(f"Контент (300 симв.): {data.get('content')[:300]}...")
            print(f"Зображення: {data.get('image_url')}")

    asyncio.run(test())
