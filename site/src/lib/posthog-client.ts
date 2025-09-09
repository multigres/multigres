import posthog from 'posthog-js'
import type { PostHogConfig } from 'posthog-js'

// max queued events while posthog initializes
const MAX_PENDING_EVENTS = 20

class PostHogClient {
  private initialized = false
  private pendingGroups: Record<string, string> = {}
  private pendingIdentification: {
    userId: string
    properties?: Record<string, any>
  } | null = null
  private pendingEvents: Array<{
    event: string
    properties: Record<string, any>
  }> = []
  private readonly maxPendingEvents = MAX_PENDING_EVENTS

  init(apiKey: string, apiHost: string) {
    if (this.initialized || typeof window === 'undefined') return

    if (!apiKey) {
      return
    }

    const config: Partial<PostHogConfig> = {
      api_host: apiHost,
      autocapture: true, // enable autocapture for all interactions
      capture_pageview: false, // manually track pageviews for spa
      capture_pageleave: false, // manually track page leaves
      persistence: 'localStorage',
      loaded: posthog => {
        // apply pending groups
        Object.entries(this.pendingGroups).forEach(([type, id]) => {
          posthog.group(type, id)
        })
        this.pendingGroups = {}

        // apply pending identification
        if (this.pendingIdentification) {
          try {
            posthog.identify(
              this.pendingIdentification.userId,
              this.pendingIdentification.properties
            )
          } catch (error) {
            if (process.env.NODE_ENV === 'development') {
              console.error('PostHog identify failed:', error)
            }
          }
          this.pendingIdentification = null
        }

        // flush pending events with sendbeacon for reliability
        this.pendingEvents.forEach(({ event, properties }) => {
          try {
            posthog.capture(event, properties, { transport: 'sendBeacon' })
          } catch (error) {
            if (process.env.NODE_ENV === 'development') {
              console.error('PostHog capture failed:', error)
            }
          }
        })
        this.pendingEvents = []
      },
    }

    posthog.init(apiKey, config)
    this.initialized = true
  }

  capturePageView(properties: Record<string, any>) {
    if (!this.initialized) {
      // queue event for when posthog initializes
      if (this.pendingEvents.length >= this.maxPendingEvents) {
        this.pendingEvents.shift() // remove oldest
      }
      this.pendingEvents.push({ event: '$pageview', properties })
      return
    }

    try {
      posthog.capture('$pageview', properties, { transport: 'sendBeacon' })
    } catch (error) {
      if (process.env.NODE_ENV === 'development') {
        console.error('PostHog pageview capture failed:', error)
      }
    }
  }

  capturePageLeave(properties: Record<string, any>) {
    if (!this.initialized) {
      // queue event for when posthog initializes
      if (this.pendingEvents.length >= this.maxPendingEvents) {
        this.pendingEvents.shift() // remove oldest
      }
      this.pendingEvents.push({ event: '$pageleave', properties })
      return
    }

    try {
      // use sendbeacon to survive tab close
      posthog.capture('$pageleave', properties, { transport: 'sendBeacon' })
    } catch (error) {
      if (process.env.NODE_ENV === 'development') {
        console.error('PostHog pageleave capture failed:', error)
      }
    }
  }

  capture(event: string, properties?: Record<string, any>) {
    if (!this.initialized) {
      // queue event for when posthog initializes
      if (this.pendingEvents.length >= this.maxPendingEvents) {
        this.pendingEvents.shift() // remove oldest
      }
      this.pendingEvents.push({ event, properties: properties || {} })
      return
    }

    try {
      posthog.capture(event, properties, { transport: 'sendBeacon' })
    } catch (error) {
      if (process.env.NODE_ENV === 'development') {
        console.error('PostHog capture failed:', error)
      }
    }
  }

  identify(userId: string, properties?: Record<string, any>) {
    if (!this.initialized) {
      // queue identification for when posthog initializes
      this.pendingIdentification = { userId, properties }
      return
    }

    try {
      posthog.identify(userId, properties)
    } catch (error) {
      if (process.env.NODE_ENV === 'development') {
        console.error('PostHog identify failed:', error)
      }
    }
  }

  isLoaded(): boolean {
    return typeof posthog !== 'undefined' && posthog.__loaded
  }
}

export const posthogClient = new PostHogClient()
