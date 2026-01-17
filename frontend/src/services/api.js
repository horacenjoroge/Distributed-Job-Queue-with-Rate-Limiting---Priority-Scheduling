import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'
const API_KEY = import.meta.env.VITE_API_KEY || ''

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
    ...(API_KEY && { 'X-API-Key': API_KEY })
  }
})

export const taskService = {
  list: (params = {}) => api.get('/tasks', { params }),
  get: (id) => api.get(`/tasks/${id}`),
  create: (data) => api.post('/tasks', data),
  cancel: (id, reason, force = false) => api.post(`/tasks/${id}/cancel`, { reason, force }),
  retry: (id) => api.post(`/tasks/${id}/retry`),
  delete: (id) => api.delete(`/tasks/${id}`)
}

export const queueService = {
  list: () => api.get('/queues'),
  getStats: (name) => api.get(`/queues/${name}/stats`),
  purge: (name) => api.delete(`/queues/${name}`)
}

export const workerService = {
  list: () => api.get('/workers'),
  get: (id) => api.get(`/workers/${id}`),
  delete: (id) => api.delete(`/workers/${id}`)
}

export const dlqService = {
  list: (params = {}) => api.get('/dlq', { params }),
  get: (id) => api.get(`/dlq/${id}`),
  retry: (id) => api.post(`/dlq/${id}/retry`),
  delete: (id) => api.delete(`/dlq/${id}`),
  purge: () => api.delete('/dlq'),
  getStats: () => api.get('/dlq/stats')
}

export const metricsService = {
  get: (windowSeconds = 3600) => api.get('/metrics', { params: { window_seconds: windowSeconds } }),
  getAggregate: (aggregation = 'hourly', timestamp = null) => {
    const params = { aggregation }
    if (timestamp) params.timestamp = timestamp
    return api.get('/metrics/aggregate', { params })
  }
}

export default api
