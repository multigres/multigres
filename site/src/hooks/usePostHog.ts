import { useLocation } from '@docusaurus/router'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'
import { useEffect, useRef } from 'react'
import { posthogClient } from '../lib/posthog-client'

export function usePostHog() {
  const { siteConfig } = useDocusaurusContext()
  const location = useLocation()
  const isInitialized = useRef(false)

  useEffect(() => {
    if (isInitialized.current) return
    if (typeof window === 'undefined') return

    const posthogKey = siteConfig.customFields?.POSTHOG_API_KEY as
      | string
      | undefined

    const apiHost = siteConfig.customFields?.POSTHOG_API_HOST as
      | string
      | undefined

    if (!posthogKey) return
    if (!apiHost) return

    // init posthog client
    posthogClient.init(posthogKey, apiHost)
    isInitialized.current = true

    // capture initial pageview
    posthogClient.capturePageView({
      $current_url: window.location.href,
      $pathname: location.pathname,
    })
  }, [siteConfig])

  // track pageviews on route change
  useEffect(() => {
    if (!isInitialized.current) return
    if (typeof window === 'undefined') return

    // capture pageview (will be queued if posthog not ready)
    posthogClient.capturePageView({
      $current_url: window.location.href,
      $pathname: location.pathname,
    })
  }, [location])

  // handle page leave
  useEffect(() => {
    const handleBeforeUnload = () => {
      posthogClient.capturePageLeave({
        $current_url: window.location.href,
        $pathname: location.pathname,
      })
    }

    window.addEventListener('beforeunload', handleBeforeUnload)
    return () => window.removeEventListener('beforeunload', handleBeforeUnload)
  }, [location.pathname])

  // return api for manual tracking
  return {
    capture: (event: string, properties?: Record<string, unknown>) => {
      posthogClient.capture(event, properties)
    },
    identify: (distinctId: string, properties?: Record<string, unknown>) => {
      posthogClient.identify(distinctId, properties)
    },
  }
}
