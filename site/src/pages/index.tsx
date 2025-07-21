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
      description="OrioleDB is a PostgreSQL extension that combines the advantages of both on-disk and in-memory engines."
    >
      <main>
        <Banner />
        <HomepageFeatures />
      </main>
    </Layout>
  )
}
