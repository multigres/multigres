import React from 'react'
import clsx from 'clsx'
import Link from '@docusaurus/Link'

import styles from './styles.module.css'

export default function HomepageSections(): React.JSX.Element {
  return (
    <div className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <div className="row">
          <div className="col col--8 padding--lg">
            <p className={clsx('hero__subtitle', styles.heroSubtitle)}>
              Multigres is Vitess for Postgre: a horizontally scalable Postgres
              architecture that supports multi-tenant, multi-writer, and
              globally distributed deployments, all while staying true to
              standard Postgres.
            </p>
          </div>
          <div className="col col--4 padding--lg">
            <div className={styles.buttons}>
              <Link
                className="button button--secondary button--lg button--block"
                to="https://github.com/multigres/multigres"
              >
                Follow us on GitHub
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
