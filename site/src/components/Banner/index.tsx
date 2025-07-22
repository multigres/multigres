import React from 'react'
import clsx from 'clsx'
import Heading from '@theme/Heading'
import Link from '@docusaurus/Link'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'

import styles from './styles.module.css'

export default function HomepageBanner(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext()
  return (
    <header
      className={clsx('hero hero--primary shadow--md', styles.heroBanner)}
    >
      <div className="container">
        <div className="row">
          <div className="col col--8">
            <Heading as="h2" className={clsx('hero__title', styles.heroTitle)}>
              {siteConfig.tagline}
            </Heading>
            <p className={clsx('hero__subtitle', styles.heroSubtitle)}>
              A horizontally scalable Postgres architecture that supports
              multi-tenant, highly-available and globally distributed deployments,
              all while staying true to standard Postgres.
            </p>
            <div className={styles.buttons}>
              <Link
                className={clsx(
                  'button button--secondary button--lg',
                  styles.primaryButton
                )}
                to="/docs"
              >
                Read the docs
              </Link>
              <Link
                className={clsx(
                  'secondary-button button button--secondary button--outline button--lg',
                  styles.secondaryButton
                )}
                to="https://github.com/multigres/multigres"
              >
                GitHub
              </Link>
            </div>
          </div>
          {/* <div className={clsx('col col--6', styles.heroImageContainer)}>
            <img
              className={styles.heroImage}
              src="/img/banner.png"
              alt="Multigres"
            />
          </div> */}
        </div>
      </div>
    </header>
  )
}
