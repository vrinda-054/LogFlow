import React, { useState } from 'react';
import { triggerFaultScenario } from '../api/logService.js';

const scenarios = [
  { label: 'Inject Latency', type: 'latency' },
  { label: 'Crash Node', type: 'node-crash' },
  { label: 'Inject Error', type: 'error' },
];

export default function ScenarioControl() {
  const [activeScenario, setActiveScenario] = useState(null);
  const [message, setMessage] = useState('');
  const [error, setError] = useState(null);

  const handleTrigger = async (scenario) => {
    setActiveScenario(scenario);
    setMessage('');
    setError(null);

    try {
      await triggerFaultScenario({ scenario });
      setMessage(`${scenario} scenario triggered.`);
    } catch (requestError) {
      setError(requestError);
    } finally {
      setActiveScenario(null);
    }
  };

  return (
    <section aria-labelledby="scenario-title">
      <h2 id="scenario-title">Fault Injection</h2>

      <div>
        {scenarios.map(({ label, type }) => (
          <button
            key={type}
            type="button"
            disabled={activeScenario !== null}
            onClick={() => handleTrigger(type)}
          >
            {activeScenario === type ? 'Triggering…' : label}
          </button>
        ))}
      </div>

      {message && <p role="status">{message}</p>}
      {error && <p role="alert">Unable to trigger scenario.</p>}
    </section>
  );
}