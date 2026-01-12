"use client";

import * as React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  IconDashboard,
  IconDatabase,
  IconFolder,
  IconInnerShadowTop,
  IconListDetails,
  IconReport,
  IconSettings,
} from "@tabler/icons-react";

import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { Logo } from "./logo";

const managementNav = [
  // { title: "Dashboard", url: "/dashboard", icon: IconDashboard },
  {
    title: "Cluster Overview",
    url: "/dashboard/topology",
    icon: IconListDetails,
  },
  { title: "Databases", url: "/dashboard/databases", icon: IconDatabase },
  // { title: "Replication", url: "/dashboard/replication", icon: IconReport },
  // {
  //   title: "Durability",
  //   url: "/dashboard/durability",
  //   icon: IconInnerShadowTop,
  // },
  // {
  //   title: "Backup & Restore",
  //   url: "/dashboard/backup-restore",
  //   icon: IconFolder,
  // },
];

const clusterNav = [
  { title: "MultiGateways", url: "/dashboard/multigateways", icon: IconFolder },
  { title: "MultiPoolers", url: "/dashboard/multipoolers", icon: IconFolder },
  { title: "MultiOrchestrator", url: "/dashboard/multiorch", icon: IconFolder },
  // { title: "Settings", url: "/dashboard/settings", icon: IconSettings },
];

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const pathname = usePathname();
  return (
    <Sidebar collapsible="offcanvas" {...props}>
      <SidebarHeader>
        <div className="px-2 flex items-center justify-between">
          <Logo size={48} />
          <div className="text-right">
            <div className="text-sm font-medium flex items-center justify-end gap-2">
              <div className="w-1 h-1 bg-green-500 rounded-full"></div>
              Cluster Online
            </div>
            <div className="text-xs text-muted-foreground">2 mins ago</div>
          </div>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Management</SidebarGroupLabel>
          <SidebarMenu>
            {managementNav.map((item) => (
              <SidebarMenuItem key={item.title}>
                <SidebarMenuButton
                  asChild
                  isActive={pathname.startsWith(item.url)}
                >
                  <Link className="text-muted-foreground" href={item.url}>
                    {item.icon && <item.icon />}
                    <span>{item.title}</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Services</SidebarGroupLabel>
          <SidebarMenu>
            {clusterNav.map((item) => (
              <SidebarMenuItem key={item.title}>
                <SidebarMenuButton
                  asChild
                  isActive={pathname.startsWith(item.url)}
                >
                  <Link className="text-muted-foreground" href={item.url}>
                    {item.icon && <item.icon />}
                    <span>{item.title}</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
