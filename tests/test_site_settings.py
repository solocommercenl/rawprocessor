import pytest
import asyncio
from site_settings import get_site_settings, SiteSettingsError, REQUIRED_FIELDS

# Make sure to use a known site in your autodex.site_settings collection, e.g. 'solostaging.nl'
TEST_SITE_URL = "solostaging.nl"

@pytest.mark.asyncio
async def test_get_valid_site_settings():
    settings = await get_site_settings(TEST_SITE_URL, use_cache=False)
    assert isinstance(settings, dict)
    for field in REQUIRED_FIELDS:
        assert field in settings

@pytest.mark.asyncio
async def test_missing_site_raises():
    with pytest.raises(SiteSettingsError):
        await get_site_settings("this-site-does-not-exist.nl", use_cache=False)

@pytest.mark.asyncio
async def test_missing_required_fields(monkeypatch):
    # Patch fetch_site_settings to simulate a settings doc missing required fields
    from site_settings import fetch_site_settings
    async def fake_fetch(site_url):
        return {"site_url": site_url, "filter_criteria": {}, "translation_profile": "default"}
    monkeypatch.setattr("site_settings.fetch_site_settings", fake_fetch)
    with pytest.raises(SiteSettingsError):
        await get_site_settings("solostaging.nl", use_cache=False)

@pytest.mark.asyncio
async def test_invalid_filter_criteria(monkeypatch):
    # Patch fetch_site_settings to simulate invalid filter_criteria
    from site_settings import fetch_site_settings
    async def fake_fetch(site_url):
        # All fields present, but filter_criteria is wrong type
        doc = {field: "dummy" for field in REQUIRED_FIELDS}
        doc["filter_criteria"] = "not_a_dict"
        doc["site_url"] = site_url
        return doc
    monkeypatch.setattr("site_settings.fetch_site_settings", fake_fetch)
    with pytest.raises(SiteSettingsError):
        await get_site_settings("solostaging.nl", use_cache=False)
