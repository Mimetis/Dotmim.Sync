export const convertToDate = (date: string) => {
    if (!date) return "";

    const d = new Date(date);

    return d.toLocaleDateString() + " " + d.toLocaleTimeString();
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

export const convertTypeToThemeColor = (type: string) => {
    switch (type) {
        case "Reinitialize":
        case "ReinitializeWithUpload":
            return "secondary";
        default:
            return "primary";
    }
};


export const convertToTime = (s:number) => {
  const ms = s % 1000;
  s = (s - ms) / 1000;
  const secs = s % 60;
  s = (s - secs) / 60;
  const mins = s % 60;
  const hrs = (s - mins) / 60;

  return hrs + ':' + mins + ':' + secs;
}