import React from 'react'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'
import Layout from '@theme/Layout'

import Banner from '@site/src/components/Banner'
import HomepageFeatures from '@site/src/components/HomepageFeatures'

export default function Home(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext()
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="A horizontally scalable Postgres architecture that supports multi-tenant, multi-writer, and globally distributed deployments, all while staying true to standard Postgres."
    >
      <main>
        <Banner />
        <HomepageFeatures />
      </main>
    </Layout>
  )
}
