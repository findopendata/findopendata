var API_ENDPOINT = '/api';
if (process.env.NODE_ENV === "development") {
    API_ENDPOINT = 'http://localhost:8080/api';
}

export default API_ENDPOINT;