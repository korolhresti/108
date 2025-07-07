-- Таблиця користувачів
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_admin BOOLEAN DEFAULT FALSE,
    last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    language TEXT DEFAULT 'uk',
    auto_notifications BOOLEAN DEFAULT FALSE,
    digest_frequency TEXT DEFAULT 'daily',
    safe_mode BOOLEAN DEFAULT FALSE,
    current_feed_id INTEGER, -- Може бути NULL
    is_premium BOOLEAN DEFAULT FALSE,
    premium_expires_at TIMESTAMP WITH TIME ZONE, -- Може бути NULL
    level INTEGER DEFAULT 1,
    badges JSONB DEFAULT '[]'::jsonb, -- Зберігаємо як JSONB для гнучкості
    inviter_id INTEGER REFERENCES users(id) ON DELETE SET NULL, -- Може бути NULL
    view_mode TEXT DEFAULT 'detailed',
    premium_invite_count INTEGER DEFAULT 0,
    digest_invite_count INTEGER DEFAULT 0,
    is_pro BOOLEAN DEFAULT FALSE,
    ai_requests_today INTEGER DEFAULT 0,
    ai_last_request_date DATE DEFAULT CURRENT_DATE
);

-- Таблиця джерел новин
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, -- Може бути NULL для системних джерел
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    normalized_source_url TEXT UNIQUE NOT NULL, -- Додано для унікальності та нормалізації
    source_type TEXT NOT NULL, -- 'web', 'rss', 'telegram', 'social_media'
    status TEXT DEFAULT 'active', -- 'active', 'inactive'
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_parsed TIMESTAMP WITH TIME ZONE -- Може бути NULL
);

-- Таблиця новин
CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    source_url TEXT NOT NULL,
    normalized_source_url TEXT UNIQUE NOT NULL, -- Додано для унікальності та нормалізації
    image_url TEXT, -- Може бути NULL
    ai_summary TEXT, -- Може бути NULL
    ai_classified_topics JSONB, -- Зберігаємо як JSONB для гнучкості списку тем
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    moderation_status TEXT DEFAULT 'pending', -- 'pending', 'approved', 'rejected'
    expires_at TIMESTAMP WITH TIME ZONE, -- Може бути NULL (для новин з обмеженим терміном дії)
    is_published_to_channel BOOLEAN DEFAULT FALSE
);

-- Таблиця переглядів новин користувачами (для відстеження прочитаних новин)
CREATE TABLE IF NOT EXISTS user_news_views (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    news_id INTEGER REFERENCES news(id) ON DELETE CASCADE,
    viewed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, news_id) -- Забезпечує унікальність перегляду для пари користувач-новина
);

-- Таблиця реакцій користувачів на новини
CREATE TABLE IF NOT EXISTS user_news_reactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    news_id INTEGER REFERENCES news(id) ON DELETE CASCADE,
    reaction_type TEXT NOT NULL, -- 'interesting', 'not_much', 'delete'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, news_id) -- Дозволяє лише одну реакцію на новину від користувача
);

-- Таблиця закладок користувачів
CREATE TABLE IF NOT EXISTS bookmarks (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    news_id INTEGER REFERENCES news(id) ON DELETE CASCADE,
    bookmarked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, news_id)
);

-- Таблиця скарг/репортів
CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    target_type TEXT NOT NULL, -- 'news', 'comment', etc.
    target_id INTEGER NOT NULL,
    reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'pending' -- 'pending', 'resolved', 'rejected'
);

-- Таблиця запрошень
CREATE TABLE IF NOT EXISTS invitations (
    id SERIAL PRIMARY KEY,
    inviter_user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    invite_code VARCHAR(8) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    used_at TIMESTAMP WITH TIME ZONE, -- Може бути NULL, якщо запрошення ще не використано
    status TEXT DEFAULT 'pending', -- 'pending', 'accepted', 'revoked'
    invitee_telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL -- Змінено на telegram_id
);

-- Таблиця статистики джерел
CREATE TABLE IF NOT EXISTS source_stats (
    source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    publication_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблиця підписок користувачів на теми
CREATE TABLE IF NOT EXISTS user_subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    topic TEXT NOT NULL,
    subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, topic) -- Забезпечує унікальність підписки на тему для користувача
);
