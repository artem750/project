import Button from '@mui/material/Button';
import KeyboardReturnRoundedIcon from '@mui/icons-material/KeyboardReturnRounded';
import * as React from 'react';

interface ButtonProps {
    title: string;
    onClick: () => void;
    style?: React.CSSProperties;
}

const ButtonBack: React.FC<ButtonProps> = ({ title, onClick, style }) => {
    return (
        <Button style={style} onClick={onClick} variant="contained" color="warning" endIcon={<KeyboardReturnRoundedIcon />}>
            {title}
        </Button>
    );
};
export default ButtonBack;