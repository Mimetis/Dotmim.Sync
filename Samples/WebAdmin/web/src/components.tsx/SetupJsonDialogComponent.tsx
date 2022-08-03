import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle } from "@mui/material";
import { useEffect, useRef } from "react";
import JSONPretty from 'react-json-pretty';
import 'react-json-pretty/themes/acai.css';

interface ISetupJsonDialogProps {

  jsonString?: string;
  open: boolean;
  setOpen: React.Dispatch<React.SetStateAction<boolean>>;
}


const SetupJsonDialogComponent: React.FunctionComponent<ISetupJsonDialogProps> = (props) => {

  const handleClose = () => { props.setOpen(false) };
  const descriptionElementRef = useRef<HTMLElement>(null);

  useEffect(() => {
    console.log("SetupJsonDialogComponent open: " + open);
    if (open) {
      const { current: descriptionElement } = descriptionElementRef;
      if (descriptionElement !== null) {
        descriptionElement.focus();
      }
    }
  }, []);

  return (

    <Dialog
      sx={{ '& .MuiDialog-paper': { width: '100%', maxHeight: 835 } }}
      maxWidth="lg"    
      fullWidth
      open={props.open}
      onClose={handleClose}
      scroll="paper"
      aria-labelledby="scroll-dialog-title"
      aria-describedby="scroll-dialog-description"
    >
      <DialogTitle id="scroll-dialog-title">Setup</DialogTitle>
      <DialogContent dividers>
        <DialogContentText
          id="scroll-dialog-description"
          ref={descriptionElementRef}
          tabIndex={-1}>
            <JSONPretty id="json-pretty" data={props.jsonString}></JSONPretty>
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose}>Close</Button>
      </DialogActions>
    </Dialog>
  )
};

export default SetupJsonDialogComponent;