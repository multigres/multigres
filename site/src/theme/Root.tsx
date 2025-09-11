import React from 'react'
import { usePostHog } from '@site/src/hooks/usePostHog'

export default function Root({ children }: { children: React.ReactNode }) {
  // init posthog and handle pageview tracking
  usePostHog()

  return <>{children}</>
}
