import * as React from "react";
import { Route, Routes } from "react-router-dom";
import { useNavigationLinks } from "../hooks";

const RoutesComponent: React.FunctionComponent = () => {
    const links = useNavigationLinks();

    return (
        <Routes>
            {links.map((link) => (
                <Route path={link.path} element={link.element} key={link.id} />
            ))}
        </Routes>
    );
};

export default RoutesComponent;
