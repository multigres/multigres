import { SiteHeader } from "@/components/site-header";

type BreadcrumbItem = {
  label: string;
  href?: string;
};

export function PageLayout({
  title,
  subTitle,
  actions,
  breadcrumbs,
  children,
}: {
  title?: string;
  subTitle?: string;
  actions?: React.ReactNode;
  breadcrumbs?: BreadcrumbItem[];
  children: React.ReactNode;
}) {
  return (
    <>
      <SiteHeader breadcrumbs={breadcrumbs} />
      <div className="flex flex-1 flex-col">
        <div className="@container/main flex flex-1 flex-col gap-0">
          {title && (
            <div className="px-4 lg:px-6 py-6">
              <div className="flex items-start justify-between gap-4">
                <div className="space-y-1">
                  <h1 className="text-2xl font-mono uppercase">{title}</h1>
                  {subTitle ? (
                    <p className="text-muted-foreground">{subTitle}</p>
                  ) : null}
                </div>
                {actions ? <div className="shrink-0">{actions}</div> : null}
              </div>
            </div>
          )}
          {children}
        </div>
      </div>
    </>
  );
}
