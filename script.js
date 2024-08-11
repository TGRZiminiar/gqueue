import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 10,
    duration: '5s',
};

export default function () {
    const url = 'http://localhost:3000/process';
    const payload = JSON.stringify({
        "id": String(Math.floor(Math.random() * 1000)),
        "topic": "mix" + "1",
        "data": "hello"
    });
    //String(Math.floor(Math.random() * 5))

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, payload, params);
    console.log(res)
    check(res, {
        'status is 200': (r) => r.status === 200,
    });

    sleep(1);
}