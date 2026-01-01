// --- DEPENDENCIES ---
// Run this in terminal first: npm install @supabase/supabase-js axios date-fns
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { addDays, format } = require('date-fns');

// --- CONFIGURATION ---
// FOR LOCAL TESTING: Paste your keys inside the quotes below.
// FOR GITHUB ACTIONS: Keep the process.env part.
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://pcflisikfdnsgmtlpgeb.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'sbp_b95d672b44f14b2f288feaef7454b49ccef12927';

if (!SUPABASE_URL || !SUPABASE_KEY || SUPABASE_URL.includes('YOUR_')) {
    console.error("❌ ERROR: Missing Supabase Credentials.");
    console.error("   -> If running locally, edit the top of ingest.js with your keys.");
    console.error("   -> If on GitHub, check your Repository Secrets.");
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Your "Secret Sauce" Logic (The Agent's Standards)
const LIMITS = { maxTemp: 85, minTemp: 55, maxWind: 15, maxRain: 30 };

// --- MAIN AGENT LOOP ---
async function runIngestion() {
    console.log("🐶 Dog Beach Scout Agent: Waking up...");

    // 1. Get all Active Locations from Database
    const { data: locations, error } = await supabase
        .from('locations')
        .select('*')
        .eq('is_active', true);

    if (error) {
        console.error("❌ Fatal DB Error:", error.message);
        return;
    }

    console.log(`📍 Found ${locations.length} active locations.`);

    // 2. Scout each location
    for (const beach of locations) {
        console.log(`\n🌊 Scouting: ${beach.display_name} (${beach.location_id})...`);
        await processLocation(beach);
    }

    console.log("\n✅ Mission Complete. Going back to sleep.");
}

// --- LOCATION PROCESSOR ---
async function processLocation(beach) {
    try {
        // A. Fetch External Data (The Senses)
        console.log("   --> Fetching Forecast & Tides...");
        
        // Parallel fetch for speed
        const [weather, tides, waterTemp] = await Promise.all([
            fetchWeather(beach.latitude, beach.longitude),
            fetchTides(beach.noaa_station_id),
            fetchWaterTemp(beach.noaa_station_id)
        ]);
        
        const finalWaterTemp = waterTemp || 60; // Fallback if sensor is down

        // B. Process & Save Hourly Data (The Details)
        console.log("   --> Processing Hourly Details...");
        const hourlyRows = processHourlyData(beach.location_id, weather, tides);
        
        const { error: hError } = await supabase
            .from('hourly_details')
            .upsert(hourlyRows, { onConflict: 'id' }); // Note: Schema might need composite constraint or ID handling
        
        // *Better Fix for Supabase*: Delete old hourly data for these dates first to avoid ID conflicts, 
        // or just let Supabase handle new IDs. For simplicity here, we assume standard insert.
        // Actually, upserting by ID is tricky if ID is random. 
        // STRATEGY CHANGE: We will just INSERT fresh data. 
        // Real production apps usually DELETE 'future' data for this location first, then INSERT new.
        
        // Let's do the "Clean & Replace" strategy for the next 7 days to keep it clean.
        const todayISO = new Date().toISOString();
        await supabase.from('hourly_details').delete().eq('location_id', beach.location_id).gte('timestamp', todayISO);
        
        const { error: insertError } = await supabase.from('hourly_details').insert(hourlyRows);
        
        if(insertError) console.error("   ❌ Hourly Save Failed:", insertError.message);
        else console.log(`   ✅ Saved ${hourlyRows.length} hourly records.`);

        // C. Process & Save Daily Summaries (The Big Picture)
        console.log("   --> Calculating Daily Summaries...");
        const dailyRows = processDailyData(beach.location_id, weather, finalWaterTemp, hourlyRows);
        
        const { error: dError } = await supabase
            .from('daily_summaries')
            .upsert(dailyRows, { onConflict: 'location_id, date' });

        if(dError) console.error("   ❌ Daily Save Failed:", dError.message);
        else console.log(`   ✅ Saved ${dailyRows.length} daily summaries.`);

    } catch (e) {
        console.error(`   💀 CRITICAL FAILURE for ${beach.location_id}:`, e.message);
    }
}

// --- LOGIC ENGINES ---

function processHourlyData(locationId, weather, tides) {
    const rows = [];
    const timeArray = weather.hourly.time;
    
    // Create a map for quick tide lookups (Round tide timestamp to nearest hour)
    const tideMap = {};
    tides.forEach(t => {
        const d = new Date(t.t);
        d.setMinutes(0,0,0);
        tideMap[d.toISOString()] = parseFloat(t.v);
    });

    for (let i = 0; i < timeArray.length; i++) {
        const ts = timeArray[i]; // ISO String from Open-Meteo
        
        // Standardize timestamp for lookup
        const lookupDate = new Date(ts).toISOString();
        
        // Find tide (or interpolate/fallback)
        // NOAA tides are specific. If exact hour missing, use 0 or nearest.
        let tideVal = tideMap[lookupDate];
        if (tideVal === undefined) tideVal = 0; 

        rows.push({
            location_id: locationId,
            timestamp: ts,
            temp_air: Math.round(weather.hourly.temperature_2m[i]),
            temp_feels_like: Math.round(weather.hourly.apparent_temperature[i]),
            humidity: weather.hourly.relative_humidity_2m[i],
            wind_speed: Math.round(weather.hourly.wind_speed_10m[i]),
            precip_chance: weather.hourly.precipitation_probability[i],
            uv_index: weather.hourly.uv_index[i],
            tide_height: tideVal
        });
    }
    return rows;
}

function processDailyData(locationId, weather, waterTemp, hourlyRows) {
    const dailyMap = new Map();

    // Group hourly rows by Date String (YYYY-MM-DD)
    hourlyRows.forEach(row => {
        const dateStr = row.timestamp.split('T')[0];
        if (!dailyMap.has(dateStr)) dailyMap.set(dateStr, []);
        dailyMap.get(dateStr).push(row);
    });

    const summaries = [];
    const dailyIndices = weather.daily.time; // Array of dates

    for (let i = 0; i < dailyIndices.length; i++) {
        const dateStr = dailyIndices[i];
        const dayHours = dailyMap.get(dateStr) || [];
        
        if (dayHours.length === 0) continue;

        // Calc Averages/Max from Hourly
        const windMax = Math.max(...dayHours.map(h => h.wind_speed));
        const humidAvg = Math.round(dayHours.reduce((sum, h) => sum + h.humidity, 0) / dayHours.length);

        // --- THE "SECRET SAUCE" ALGORITHM ---
        let score = 100;
        if (windMax > LIMITS.maxWind) score -= 20;
        if (weather.daily.temperature_2m_max[i] > LIMITS.maxTemp) score -= 20;
        if (weather.daily.temperature_2m_min[i] < LIMITS.minTemp) score -= 10;
        if (weather.daily.precipitation_probability_max[i] > 10) score -= 40;
        score = Math.max(0, score);

        // Crowd Logic
        const dayOfWeek = new Date(dateStr).getDay(); // 0=Sun, 6=Sat
        let crowd = "Quiet";
        if (dayOfWeek === 0 || dayOfWeek === 6) crowd = "Party"; // Weekends
        else if (score > 80) crowd = "Moderate"; // Nice weekdays

        summaries.push({
            location_id: locationId,
            date: dateStr,
            sunrise_ts: weather.daily.sunrise[i],
            sunset_ts: weather.daily.sunset[i],
            temp_air_max: Math.round(weather.daily.temperature_2m_max[i]),
            temp_air_min: Math.round(weather.daily.temperature_2m_min[i]),
            temp_water_avg: waterTemp, 
            humidity_avg: humidAvg,
            uv_max: weather.daily.uv_index_max[i],
            wind_max: windMax,
            rating_score: score,
            crowd_level: crowd
        });
    }
    return summaries;
}

// --- FETCHERS ---

async function fetchWeather(lat, lon) {
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&hourly=temperature_2m,apparent_temperature,relative_humidity_2m,precipitation_probability,wind_speed_10m,uv_index&daily=temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,precipitation_probability_max&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=auto&forecast_days=7`;
    const res = await axios.get(url);
    return res.data;
}

async function fetchTides(stationId) {
    // 7 Days of Tide Predictions
    const today = format(new Date(), 'yyyyMMdd');
    const future = format(addDays(new Date(), 7), 'yyyyMMdd');
    const url = `https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=${today}&end_date=${future}&station=${stationId}&product=predictions&datum=MLLW&time_zone=lst_ldt&interval=h&units=english&application=DataAPI_Sample&format=json`;
    
    try {
        const res = await axios.get(url);
        return res.data.predictions || [];
    } catch (e) {
        console.warn("⚠️ Tide fetch warning:", e.message);
        return [];
    }
}

async function fetchWaterTemp(stationId) {
    // NOAA Sensor Data (Latest)
    const url = `https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=latest&station=${stationId}&product=water_temperature&units=english&time_zone=lst_ldt&application=DataAPI_Sample&format=json`;
    
    try {
        const res = await axios.get(url);
        if (res.data.data && res.data.data.length > 0) {
            return Math.round(parseFloat(res.data.data[0].v));
        }
    } catch (e) {
        console.warn("⚠️ Water temp fetch failed (Sensor likely offline).");
    }
    return null; 
}

// --- START ---
runIngestion();
