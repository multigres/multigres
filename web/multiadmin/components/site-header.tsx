import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { ThemeToggle } from "@/components/theme-toggle";

type BreadcrumbEntry = { label: string; href?: string };

export function SiteHeader({
  breadcrumbs,
}: {
  breadcrumbs?: BreadcrumbEntry[];
}) {
  return (
    <header className="flex h-(--header-height) shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-(--header-height)">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mx-2 data-[orientation=vertical]:h-4"
        />
        {breadcrumbs && breadcrumbs.length ? (
          <Breadcrumb>
            <BreadcrumbList>
              {breadcrumbs.flatMap((bc, idx) => {
                const item = (
                  <BreadcrumbItem key={`item-${idx}`}>
                    {bc.href && idx < breadcrumbs.length - 1 ? (
                      <BreadcrumbLink href={bc.href}>{bc.label}</BreadcrumbLink>
                    ) : (
                      <BreadcrumbPage>{bc.label}</BreadcrumbPage>
                    )}
                  </BreadcrumbItem>
                );
                const separator =
                  idx < breadcrumbs.length - 1 ? (
                    <BreadcrumbSeparator key={`sep-${idx}`} />
                  ) : null;
                return [item, separator].filter(Boolean);
              })}
            </BreadcrumbList>
          </Breadcrumb>
        ) : (
          <h1 className="text-base font-medium">Documents</h1>
        )}
        <div className="ml-auto">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
