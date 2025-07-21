import React from 'react'
import clsx from 'clsx'
import Heading from '@theme/Heading'
import styles from './styles.module.css'
import { icons } from 'lucide-react'

type FeatureItem = {
  title: string
  icon: string
  description: React.JSX.Element
}

const FeatureList: FeatureItem[] = [
  {
    title: 'Horizontal Sharding',
    icon: 'Grid3x3',
    description: (
      <>
        Split large databases across multiple servers with automatic shard
        management.
      </>
    ),
  },
  {
    title: 'Connection Pooling',
    icon: 'Layers',
    description: (
      <>
        Built-in connection pooling reduces overhead and improves performance
        for high-traffic applications.
      </>
    ),
  },
  {
    title: 'Zero-Downtime Migrations',
    icon: 'Replace',
    description: (
      <>
        Seamlessly migrate tables between databases and Postgres versions
        without service interruption.
      </>
    ),
  },
  {
    title: 'High Availability',
    icon: 'Network',
    description: (
      <>
        Automatic failover and replica promotion ensure your database stays
        online when servers fail.
      </>
    ),
  },
  {
    title: 'Query Routing',
    icon: 'Waypoints',
    description: (
      <>
        Smart query distribution across shards and replicas optimizes
        performance and resource usage.
      </>
    ),
  },
  {
    title: 'Cloud-Native Architecture',
    icon: 'Container',
    description: (
      <>
        Kubernetes-ready design with automated backups and cross-zone cluster
        management.
      </>
    ),
  },
]

function Feature({ title, icon, description }: FeatureItem) {
  return (
    <div className={clsx('col col--4 padding--md')}>
      <div className={clsx('card shadow--md')}>
        <div className="card__header ">
          <div className="">
            <Icon size={28} name={icon} />
          </div>
          <Heading as="h3">{title}</Heading>
        </div>
        <div className="card__body">
          <p>{description}</p>
        </div>
      </div>
    </div>
  )
}

const Icon = ({ name, size }) => {
  const LucideIcon = icons[name]

  return <LucideIcon size={size} style={{ color: 'grey' }} />
}

export default function HomepageFeatures(): React.JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  )
}
