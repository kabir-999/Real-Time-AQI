import os
import psycopg2
import json
from datetime import datetime, timezone, timedelta
import http.server
import socketserver
import threading
import webbrowser
import time

# Set IST time zone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Database configuration from environment variables
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5433')
DB_NAME = os.getenv('POSTGRES_DB', 'aqi_db')
DB_USER = os.getenv('POSTGRES_USER', 'aqi_user')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'aqi_password')

class AQIViewer:
    def __init__(self):
        self.data = self.load_data()

    def load_data(self):
        """Load AQI data from PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )

            cursor = conn.cursor()

            # Get latest data
            cursor.execute("""
                SELECT region, AVG(avg_aqi) as avg_aqi, COUNT(*) as count, AVG(avg_temperature) as avg_temp, AVG(avg_humidity) as avg_humidity, AVG(avg_pm25) as avg_pm25, AVG(avg_pm10) as avg_pm10, AVG(avg_no2) as avg_no2, AVG(avg_so2) as avg_so2, AVG(avg_co) as avg_co, AVG(avg_o3) as avg_o3 FROM realtime_metrics_enhanced WHERE region NOT IN ('Unknown', 'test') AND avg_aqi > 0 GROUP BY region ORDER BY avg_aqi DESC
            """)

            regions_data = cursor.fetchall()

            # Get recent records
            cursor.execute("""
                SELECT window_start, region, avg_aqi, avg_pm25, avg_pm10, avg_temperature, avg_humidity, avg_no2, avg_so2, avg_co, avg_o3 FROM realtime_metrics_enhanced WHERE region NOT IN ('Unknown', 'test') AND avg_aqi > 0 ORDER BY window_start DESC LIMIT 20
            """)

            recent_data = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                'regions': [{'region': row[0], 'aqi': float(row[1]), 'count': row[2], 'temp': float(row[3]), 'humidity': float(row[4]), 'pm25': float(row[5]), 'pm10': float(row[6]), 'no2': float(row[7]), 'so2': float(row[8]), 'co': float(row[9]), 'o3': float(row[10])} for row in regions_data],
                'recent': [{'time': (row[0].replace(tzinfo=timezone.utc).astimezone(IST)).strftime('%Y-%m-%d %H:%M:%S IST'), 'region': row[1], 'aqi': float(row[2]), 'pm25': float(row[3]), 'pm10': float(row[4]), 'temp': float(row[5]), 'humidity': float(row[6]), 'no2': float(row[7]), 'so2': float(row[8]), 'co': float(row[9]), 'o3': float(row[10])} for row in recent_data],
                'last_updated': datetime.now(IST).isoformat()
            }

        except Exception as e:
            return {'error': str(e)}

    def generate_html(self):
        """Generate HTML dashboard"""
        data = self.data

        if 'error' in data:
            return f"""
            <html>
            <head><title>AQI Dashboard - Error</title></head>
            <body style="font-family: Arial; margin: 20px;">
                <h1>❌ Error Loading Data</h1>
                <p>{data['error']}</p>
                <p>Make sure your PostgreSQL database is running and the bridge script is active.</p>
            </body>
            </html>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>AQI Data Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
                .stats {{ display: flex; justify-content: space-around; margin: 20px 0; }}
                .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; min-width: 150px; }}
                .aqi-good {{ color: #28a745; font-weight: bold; }}
                .aqi-moderate {{ color: #ffc107; font-weight: bold; }}
                .aqi-unhealthy {{ color: #dc3545; font-weight: bold; }}
                table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f8f9fa; font-weight: bold; }}
                .refresh-btn {{ background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; }}
                .refresh-btn:hover {{ background: #0056b3; }}
            </style>
            <script>
                function refreshData() {{
                    location.reload();
                }}

                setInterval(function() {{
                    fetch('/api/data')
                        .then(response => response.json())
                        .then(data => {{
                            if (data.last_updated) {{
                                const serverTime = new Date(data.last_updated);
                                const istTime = new Date(serverTime.getTime() + (5.5 * 60 * 60 * 1000));
                                document.getElementById('last-updated').textContent = 'Last updated: ' + istTime.toLocaleTimeString('en-IN', {{timeZone: 'Asia/Kolkata'}}) + ' IST';
                            }}
                        }});
                }}, 10000);
            </script>
        </head>
        <body>
            <div class="header">
                <h1>AQI Real-time Dashboard</h1>
                <p>Live air quality monitoring across regions</p>
                <button class="refresh-btn" onclick="refreshData()">Refresh</button>
            </div>

            <div class="stats">
                <div class="stat-card">
                    <h3>Total Records</h3>
                    <div style="font-size: 24px; font-weight: bold;">{len(data['recent'])}</div>
                </div>
                <div class="stat-card">
                    <h3>Regions</h3>
                    <div style="font-size: 24px; font-weight: bold;">{len(data['regions'])}</div>
                </div>
                <div class="stat-card">
                    <h3>High AQI Areas</h3>
                    <div style="font-size: 24px; font-weight: bold;">{sum(1 for r in data['regions'] if r['aqi'] > 100)}</div>
                </div>
            </div>

            <h2>Regional AQI Rankings</h2>
            <table>
                <tr>
                    <th>Rank</th>
                    <th>Region</th>
                    <th>Average AQI</th>
                    <th>Status</th>
                    <th>Data Points</th>
                    <th>Temperature</th>
                    <th>Humidity</th>
                    <th>PM2.5</th>
                    <th>PM10</th>
                    <th>NO2</th>
                    <th>SO2</th>
                    <th>CO</th>
                    <th>O3</th>
                </tr>
        """

        aqi_colors = {
            'Good': '#28a745',
            'Moderate': '#ffc107',
            'Unhealthy': '#dc3545'
        }

        for i, region in enumerate(data['regions'], 1):
            aqi = region['aqi']
            if aqi <= 50:
                status = 'Good'
                color = aqi_colors['Good']
            elif aqi <= 100:
                status = 'Moderate'
                color = aqi_colors['Moderate']
            else:
                status = 'Unhealthy'
                color = aqi_colors['Unhealthy']

            html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{region['region']}</td>
                    <td><span style="color: {color}; font-weight: bold;">{aqi:.1f}</span></td>
                    <td>
                        <span style="background: {color}; color: white; padding: 3px 8px; border-radius: 12px; font-size: 12px;">
                            {status}
                        </span>
                    </td>
                    <td>{region['count']}</td>
                    <td>{region.get('temp', 0):.1f}°C</td>
                    <td>{region.get('humidity', 0):.1f}%</td>
                    <td>{region.get('pm25', 0):.1f}</td>
                    <td>{region.get('pm10', 0):.1f}</td>
                    <td>{region.get('no2', 0):.2f}</td>
                    <td>{region.get('so2', 0):.2f}</td>
                    <td>{region.get('co', 0):.2f}</td>
                    <td>{region.get('o3', 0):.2f}</td>
                </tr>
            """

        html += """
            </table>

            <h2>Recent Measurements</h2>
            <table>
                <tr>
                    <th>Time</th>
                    <th>Region</th>
                    <th>AQI</th>
                    <th>PM2.5</th>
                    <th>PM10</th>
                    <th>Temp</th>
                    <th>Humidity</th>
                    <th>NO2</th>
                    <th>SO2</th>
                    <th>CO</th>
                    <th>O3</th>
                </tr>
        """

        for record in data['recent'][:10]:  # Show last 10
            aqi = record['aqi']
            if aqi <= 50:
                aqi_class = 'aqi-good'
            elif aqi <= 100:
                aqi_class = 'aqi-moderate'
            else:
                aqi_class = 'aqi-unhealthy'

            html += f"""
                <tr>
                    <td>{record['time']}</td>
                    <td>{record['region']}</td>
                    <td><span class="{aqi_class}">{record['aqi']:.1f}</span></td>
                    <td>{record.get('pm25', 0):.1f}</td>
                    <td>{record.get('pm10', 0):.1f}</td>
                    <td>{record.get('temp', 0):.1f}°C</td>
                    <td>{record.get('humidity', 0):.1f}%</td>
                    <td>{record.get('no2', 0):.2f}</td>
                    <td>{record.get('so2', 0):.2f}</td>
                    <td>{record.get('co', 0):.2f}</td>
                    <td>{record.get('o3', 0):.2f}</td>
                </tr>
            """


        return html

class AQIHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/dashboard':
            # Create fresh data for each request
            viewer = AQIViewer()
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(viewer.generate_html().encode())
        elif self.path == '/api/data':
            # Create fresh data for each API request
            viewer = AQIViewer()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(viewer.load_data()).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Page not found')

def start_server():
    """Start the web server"""
    with socketserver.TCPServer(("", 8000), AQIHTTPRequestHandler) as httpd:
        print("AQI Dashboard running at: http://localhost:8000")
        print("Auto-refreshing every 10 seconds")
        print("Press Ctrl+C to stop")
        httpd.serve_forever()

if __name__ == "__main__":
    # Start server in background thread
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    # Open browser
    time.sleep(1)
    webbrowser.open('http://localhost:8000')

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n✅ Server stopped")
