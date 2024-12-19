import Button from '@mui/material/Button';
import SendIcon from '@mui/icons-material/Send';
import * as React from 'react';

interface ButtonProps {
    title: string;
    onClick: () => void;
    style?: React.CSSProperties;
}

const ButtonSend: React.FC<ButtonProps> = ({ title, onClick, style }) => {
    return (
        <Button style={style} onClick={onClick} variant="contained" endIcon={<SendIcon />}>
            {title}
        </Button>
    );
};
export default ButtonSend;
