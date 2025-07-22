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
      description="Multigres is a project to build an adaptation of Vitess for Postgres."
    >
      <main>
        <Banner />
        <HomepageFeatures />
      </main>
    </Layout>
  )
}
