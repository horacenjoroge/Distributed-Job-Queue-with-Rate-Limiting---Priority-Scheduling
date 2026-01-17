import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Home from './pages/Home'
import Tasks from './pages/Tasks'
import TaskDetail from './pages/TaskDetail'
import Queues from './pages/Queues'
import Workers from './pages/Workers'
import DLQ from './pages/DLQ'
import Metrics from './pages/Metrics'

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/tasks" element={<Tasks />} />
          <Route path="/tasks/:id" element={<TaskDetail />} />
          <Route path="/queues" element={<Queues />} />
          <Route path="/workers" element={<Workers />} />
          <Route path="/dlq" element={<DLQ />} />
          <Route path="/metrics" element={<Metrics />} />
        </Routes>
      </Layout>
    </Router>
  )
}

export default App
