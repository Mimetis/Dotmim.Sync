import React, { useMemo } from "react";
import { useLocation } from "react-router-dom";
import PeopleIcon from "@mui/icons-material/People";
import DnsRoundedIcon from "@mui/icons-material/DnsRounded";
import { ClientsPage, ClientsPageDetails, ScopesPage, SyncLogsPage } from "../pages";

export type NavigationLink = {
    id: string;
    icon: React.ReactNode;
    path: string;
    active: boolean;
    element?: React.ReactNode;
};

export const useNavigationLinks = (): NavigationLink[] => {

    const location = useLocation();

    const links = useMemo(() => {
        return [
            { id: "History", element:<SyncLogsPage />, icon: <PeopleIcon />, path: "/", active: location.pathname === "/" },
            { id: "Clients", element : <ClientsPage />, icon: <PeopleIcon />, path: "/clients", active: location.pathname === "/clients" },
            { id: "ClientDetails", element : <ClientsPageDetails />,icon: <PeopleIcon />, path: "/clientDetails/:clientScopeId", active: false },
            { id: "Scopes", element : <ScopesPage />,icon: <DnsRoundedIcon />, path: "/scopes", active: location.pathname === "/scopes" },
        ];
    }, [location.pathname]);

    return links;
};
