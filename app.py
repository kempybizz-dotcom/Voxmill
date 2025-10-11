from flask import Flask, request, jsonify
import os
from voxmill_scraper import generate_market_report

app = Flask(__name__)

@app.route('/')
def home():
    return '''
    <html>
        <body style="background: #000; color: #D4AF37; font-family: sans-serif; padding: 40px;">
            <h1>VOXMILL MARKET INTELLIGENCE ENGINE</h1>
            <p>Status: Online ✅</p>
            <hr style="border-color: #D4AF37;">
            <h2>Generate Report</h2>
            <form action="/generate-report" method="POST">
                <label>City:</label><br>
                <input type="text" name="city" value="Miami" style="padding: 8px; margin: 10px 0; width: 300px;"><br>
                
                <label>State:</label><br>
                <input type="text" name="state" value="FL" style="padding: 8px; margin: 10px 0; width: 300px;"><br>
                
                <label>Property Type:</label><br>
                <select name="property_type" style="padding: 8px; margin: 10px 0; width: 316px;">
                    <option value="luxury">Luxury ($1M+)</option>
                    <option value="mid-range">Mid-Range ($500K-$1M)</option>
                    <option value="all">All Properties</option>
                </select><br>
                
                <button type="submit" style="background: #D4AF37; color: #000; padding: 12px 24px; border: none; margin-top: 20px; cursor: pointer; font-weight: bold;">
                    GENERATE REPORT
                </button>
            </form>
        </body>
    </html>
    '''

@app.route('/generate-report', methods=['POST'])
def generate_report():
    try:
        # Get parameters from form or JSON
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form
        
        city = data.get('city', 'Miami')
        state = data.get('state', 'FL')
        property_type = data.get('property_type', 'luxury')
        
        # Generate the report
        report_data = generate_market_report(city, state, property_type)
        
        return jsonify({
            'status': 'success',
            'city': city,
            'state': state,
            'report': report_data
        })
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'service': 'voxmill-report-engine'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
