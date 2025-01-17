import 'virtual:windi.css'
import './theme.less'
import './i18n'

import React, { StrictMode } from 'react'
import { render } from 'react-dom'

import App from './App'

function mountApp() {
  render(
    <StrictMode>
      <App />
    </StrictMode>,
    document.getElementById('root')
  )
}

if (import.meta.env.MODE === 'mock') {
  // @ts-ignore
  import('./mock').then(({ startWorker }) => {
    startWorker()
    mountApp()
  })
} else {
  mountApp()
}
