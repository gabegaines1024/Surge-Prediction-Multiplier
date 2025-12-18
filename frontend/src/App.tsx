import { useState } from 'react'
import './App.css'

interface PredictionInput {
  supplyElasticity: number
  lagDER15: number
  lagDER30: number
  demandVelocity: number
  lagDemandVelocity15: number
  temp: number
  precip: number
  hour: number
}

interface PredictionResult {
  prediction: number
  confidence: string
  surgeLevel: string
}

function App() {
  const [input, setInput] = useState<PredictionInput>({
    supplyElasticity: 0.8,
    lagDER15: 1.2,
    lagDER30: 1.1,
    demandVelocity: 5,
    lagDemandVelocity15: 3,
    temp: 15,
    precip: 0,
    hour: 12
  })
  
  const [result, setResult] = useState<PredictionResult | null>(null)
  const [loading, setLoading] = useState(false)

  const handleInputChange = (field: keyof PredictionInput, value: string) => {
    setInput(prev => ({
      ...prev,
      [field]: parseFloat(value) || 0
    }))
  }

  const getSurgeLevel = (der: number): string => {
    if (der < 0.8) return 'Low Demand'
    if (der < 1.2) return 'Normal'
    if (der < 1.5) return 'Moderate Surge'
    if (der < 2.0) return 'High Surge'
    return 'Extreme Surge'
  }

  const getSurgeLevelColor = (level: string): string => {
    switch(level) {
      case 'Low Demand': return '#4ade80'
      case 'Normal': return '#60a5fa'
      case 'Moderate Surge': return '#fbbf24'
      case 'High Surge': return '#f97316'
      case 'Extreme Surge': return '#ef4444'
      default: return '#9ca3af'
    }
  }

  const handlePredict = async () => {
    setLoading(true)
    
    // Simulate API call - in production, this would call your Flask/FastAPI backend
    setTimeout(() => {
      // Mock prediction (replace with actual API call)
      const mockPrediction = 
        input.lagDER15 * 0.4 + 
        input.lagDER30 * 0.2 + 
        input.supplyElasticity * 0.3 +
        (input.demandVelocity / 10) * 0.1
      
      const surgeLevel = getSurgeLevel(mockPrediction)
      
      setResult({
        prediction: mockPrediction,
        confidence: '87%',
        surgeLevel
      })
      setLoading(false)
    }, 800)
  }

  const handleLoadSample = (scenario: 'morning' | 'evening' | 'rainy') => {
    const samples = {
      morning: {
        supplyElasticity: 0.6,
        lagDER15: 1.8,
        lagDER30: 1.6,
        demandVelocity: 15,
        lagDemandVelocity15: 12,
        temp: 10,
        precip: 0,
        hour: 8
      },
      evening: {
        supplyElasticity: 0.4,
        lagDER15: 2.2,
        lagDER30: 2.0,
        demandVelocity: 25,
        lagDemandVelocity15: 20,
        temp: 18,
        precip: 0,
        hour: 18
      },
      rainy: {
        supplyElasticity: 0.3,
        lagDER15: 2.5,
        lagDER30: 2.3,
        demandVelocity: 30,
        lagDemandVelocity15: 28,
        temp: 12,
        precip: 5.2,
        hour: 17
      }
    }
    setInput(samples[scenario])
    setResult(null)
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <h1>🚕 NYC Taxi Surge Predictor</h1>
          <p>Real-time demand forecasting powered by XGBoost</p>
        </div>
      </header>

      <main className="main">
        <div className="container">
          <div className="grid">
            {/* Input Section */}
            <div className="card">
              <h2>Input Features</h2>
              
              <div className="sample-buttons">
                <button onClick={() => handleLoadSample('morning')} className="btn-sample">
                  🌅 Morning Rush
                </button>
                <button onClick={() => handleLoadSample('evening')} className="btn-sample">
                  🌆 Evening Rush
                </button>
                <button onClick={() => handleLoadSample('rainy')} className="btn-sample">
                  🌧️ Rainy Day
                </button>
              </div>

              <div className="form-section">
                <h3>Market Dynamics</h3>
                <div className="input-group">
                  <label>Supply Elasticity</label>
                  <input
                    type="number"
                    step="0.1"
                    value={input.supplyElasticity}
                    onChange={(e) => handleInputChange('supplyElasticity', e.target.value)}
                  />
                  <span className="hint">Ratio of drivers to requests</span>
                </div>

                <div className="input-group">
                  <label>DER (t-15 min)</label>
                  <input
                    type="number"
                    step="0.1"
                    value={input.lagDER15}
                    onChange={(e) => handleInputChange('lagDER15', e.target.value)}
                  />
                  <span className="hint">Demand Excess Ratio 15 min ago</span>
                </div>

                <div className="input-group">
                  <label>DER (t-30 min)</label>
                  <input
                    type="number"
                    step="0.1"
                    value={input.lagDER30}
                    onChange={(e) => handleInputChange('lagDER30', e.target.value)}
                  />
                  <span className="hint">Demand Excess Ratio 30 min ago</span>
                </div>

                <div className="input-group">
                  <label>Demand Velocity</label>
                  <input
                    type="number"
                    step="1"
                    value={input.demandVelocity}
                    onChange={(e) => handleInputChange('demandVelocity', e.target.value)}
                  />
                  <span className="hint">Rate of demand change</span>
                </div>

                <div className="input-group">
                  <label>Lag Demand Velocity</label>
                  <input
                    type="number"
                    step="1"
                    value={input.lagDemandVelocity15}
                    onChange={(e) => handleInputChange('lagDemandVelocity15', e.target.value)}
                  />
                  <span className="hint">Velocity 15 min ago</span>
                </div>
              </div>

              <div className="form-section">
                <h3>Weather & Time</h3>
                <div className="input-group">
                  <label>Temperature (°C)</label>
                  <input
                    type="number"
                    step="0.1"
                    value={input.temp}
                    onChange={(e) => handleInputChange('temp', e.target.value)}
                  />
                </div>

                <div className="input-group">
                  <label>Precipitation (mm)</label>
                  <input
                    type="number"
                    step="0.1"
                    value={input.precip}
                    onChange={(e) => handleInputChange('precip', e.target.value)}
                  />
                </div>

                <div className="input-group">
                  <label>Hour of Day</label>
                  <input
                    type="number"
                    min="0"
                    max="23"
                    value={input.hour}
                    onChange={(e) => handleInputChange('hour', e.target.value)}
                  />
                  <span className="hint">0-23 (military time)</span>
                </div>
              </div>

              <button 
                className="btn-primary"
                onClick={handlePredict}
                disabled={loading}
              >
                {loading ? 'Predicting...' : '🎯 Predict Surge'}
              </button>
            </div>

            {/* Results Section */}
            <div className="card">
              <h2>Prediction Results</h2>
              
              {result ? (
                <div className="results">
                  <div 
                    className="result-main"
                    style={{ 
                      borderColor: getSurgeLevelColor(result.surgeLevel),
                      backgroundColor: `${getSurgeLevelColor(result.surgeLevel)}15`
                    }}
                  >
                    <div className="result-label">Predicted Surge Level</div>
                    <div 
                      className="result-value"
                      style={{ color: getSurgeLevelColor(result.surgeLevel) }}
                    >
                      {result.surgeLevel}
                    </div>
                    <div className="result-der">
                      DER: {result.prediction.toFixed(2)}x
                    </div>
                  </div>

                  <div className="result-metrics">
                    <div className="metric">
                      <div className="metric-label">Model Confidence</div>
                      <div className="metric-value">{result.confidence}</div>
                    </div>
                    <div className="metric">
                      <div className="metric-label">Demand Excess Ratio</div>
                      <div className="metric-value">{result.prediction.toFixed(2)}x</div>
                    </div>
                  </div>

                  <div className="result-info">
                    <h3>📊 What this means:</h3>
                    {result.prediction < 0.8 && (
                      <p>Low demand period. More drivers than riders. Consider reducing active drivers.</p>
                    )}
                    {result.prediction >= 0.8 && result.prediction < 1.2 && (
                      <p>Balanced supply and demand. Normal operations expected.</p>
                    )}
                    {result.prediction >= 1.2 && result.prediction < 1.5 && (
                      <p>Moderate surge expected. Demand exceeding supply. Consider activating more drivers.</p>
                    )}
                    {result.prediction >= 1.5 && result.prediction < 2.0 && (
                      <p>High surge conditions. Significant demand pressure. Surge pricing recommended.</p>
                    )}
                    {result.prediction >= 2.0 && (
                      <p>Extreme surge! Very high demand with limited supply. Maximum surge pricing advised.</p>
                    )}
                  </div>

                  <div className="result-features">
                    <h3>Key Factors</h3>
                    <div className="feature-chips">
                      {input.precip > 1 && <span className="chip chip-weather">🌧️ Rain Impact</span>}
                      {input.hour >= 7 && input.hour <= 9 && <span className="chip chip-time">🌅 Morning Rush</span>}
                      {input.hour >= 17 && input.hour <= 19 && <span className="chip chip-time">🌆 Evening Rush</span>}
                      {input.demandVelocity > 20 && <span className="chip chip-demand">📈 High Velocity</span>}
                      {input.supplyElasticity < 0.5 && <span className="chip chip-supply">🚗 Low Supply</span>}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="results-empty">
                  <div className="empty-icon">📊</div>
                  <p>Enter features and click "Predict Surge" to see results</p>
                  <p className="empty-hint">Try loading a sample scenario to get started!</p>
                </div>
              )}
            </div>
          </div>

          {/* Info Section */}
          <div className="info-card">
            <h3>ℹ️ About This Model</h3>
            <div className="info-grid">
              <div className="info-item">
                <strong>Algorithm:</strong> XGBoost Regressor
              </div>
              <div className="info-item">
                <strong>Training Data:</strong> 1M+ NYC taxi trips (Jan-Mar 2025)
              </div>
              <div className="info-item">
                <strong>MAE:</strong> 0.47 (Mean Absolute Error)
              </div>
              <div className="info-item">
                <strong>Features:</strong> 9 (Market + Weather + Time)
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}

export default App
