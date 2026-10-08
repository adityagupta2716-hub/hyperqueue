import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '15s', target: 200 },  // Ramp up to 200 VUs
    { duration: '30s', target: 1000 }, // Peak load: 1000 VUs
    { duration: '15s', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<50', 'p(99)<100'], // 95% of requests must finish within 50ms
    http_req_failed: ['rate<0.001'],               // Less than 0.1% failure rate
  },
};

export default function () {
  const url = 'http://localhost:4000/api/jobs';
  const payload = JSON.stringify({
    name: 'benchmark-stress-job',
    data: {
      userId: `user_${Math.floor(Math.random() * 100000)}`,
      amount: Math.random() * 500,
    },
    priority: Math.random() > 0.8 ? 'critical' : 'normal',
    deduplicationKey: `dedup_${Math.random()}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);
  check(res, {
    'status is 201': (r) => r.status === 201,
    'has job id': (r) => JSON.parse(r.body).job?.id !== undefined,
  });

  sleep(0.01);
}
