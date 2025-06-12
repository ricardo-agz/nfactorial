import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { RunProvider } from './context/RunContext'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RunProvider>
      <App />
    </RunProvider>
  </StrictMode>,
)
