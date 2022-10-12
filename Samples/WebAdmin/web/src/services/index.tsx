import { CheckCircle, More, AddBox, Add, Error, Sync, Help } from "@mui/icons-material";

export const convertToDate = (date: string) => {
    if (!date) return "";

    const d = new Date(date);

    return d.toLocaleDateString() + " " + d.toLocaleTimeString();
};

export const getDuration = (date1: string, date2: string) => {
    if (!date1 || !date2) return 0;

    const d1 = new Date(date1);
    const d2 = new Date(date2);
    
    return d2.getTime() - d1.getTime();

}


export const convertToDurationString = (date1: string, date2: string) => {
    if (!date1 || !date2) return "";

    const d1 = new Date(date1);
    const d2 = new Date(date2);
    
    const ms = d2.getTime() - d1.getTime();
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    return `${hours}h ${minutes % 60}m ${seconds % 60}s ${ms % 1000}ms`;
}

export const convertStateToIcon: React.FunctionComponent<{ state: string }> = (props) => {
    switch (props.state.toLocaleLowerCase()) {
        case "success":
            return <CheckCircle color="success" />;
        case "error":
            return <Error color="error" />;
        case "partial":
            return <Help color="secondary" />;
        default:
            return <Sync color="secondary" />;

    }

};

export const convertTypeToString = (type: string) => {
    switch (type) {
        case "Reinitialize":
            return "R";
        case "ReinitializeWithUpload":
            return "RWU";
        default:
            return "N";
    }
};


export const convertSyncRowStateToString = (type: number | string) => {
    switch (type.toString()) {
        case "2":
            return "None";
        case "8":
            return "Delete";
        case "16":
            return "Insert/Update";
        case "32":
            return "RetryDeletedOnNextSync";
        case "64":
            return "RetryModifiedOnNextSync";
        case "128":
            return "ApplyDeletedFailed";
        case "256":
            return "ApplyModifiedFailed";
        default:
            return "Unknown";
    }
};


export const convertTypeToThemeColor = (type: string) => {
    switch (type) {
        case "Reinitialize":
        case "ReinitializeWithUpload":
            return "secondary";
        default:
            return "primary";
    }
};

export const convertToTime = (s: number) => {
    const ms = s % 1000;
    s = (s - ms) / 1000;
    const secs = s % 60;
    s = (s - secs) / 60;
    const mins = s % 60;
    const hrs = (s - mins) / 60;

    return hrs + ":" + mins + ":" + secs;
};
